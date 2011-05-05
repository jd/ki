#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# nodlehs.storage -- Git based file system storage
#
#    Copyright © 2011  Julien Danjou <julien@danjou.info>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from .fuse import FUSE
from .utils import *
from .objects import Record, Config
from .remote import BUS_INTERFACE, Remote
from .commiter import TimeCommiter
from dulwich.repo import Repo, BASE_DIRECTORIES, OBJECTDIR, DiskObjectStore
from dulwich.client import UpdateRefsError
import os
import xdg.BaseDirectory
import threading
import dbus.service

BUS_PATH = "/org/naquadah/Nodlehs"

_storage_manager = None

def get_storage_manager(bus):
    global _storage_manager
    if _storage_manager is None:
        _storage_manager = StorageManager(bus)
    return _storage_manager

class NotEmptyDirectory(Exception):
    pass

class StorageManager(dbus.service.Object):

    def __init__(self, busname):
        # XXX Singleton?
        self.storages = {}
        self.user_storage = None
        self.busname = busname
        super(StorageManager, self).__init__(busname, "%s/%s" % (BUS_PATH,
                                                                 self.__class__.__name__))

    def create_storage(self, path=None):
        if path is None:
            path = xdg.BaseDirectory.save_data_path("nodlehs/storage")
        if len(os.listdir(path)) is 0:
            return Storage.init_bare(self.busname, path)
        return Storage(self.busname, path)

    @dbus.service.method(dbus_interface="%s.StorageManager" % BUS_INTERFACE,
                         in_signature='s', out_signature='o')
    def GetStorage(self, path):
        """Create a storage."""
        if not self.storages.has_key(repo):
            self.storages[repo] = self.create_storage(path)
        return self.storages[repo].__dbus_object_path__

    @dbus.service.method(dbus_interface="%s.StorageManager" % BUS_INTERFACE,
                         out_signature='o')
    def GetUserStorage(self):
        """Create the default user storage."""
        if self.user_storage is None:
            self.user_storage = self.create_storage()
        return self.user_storage.__dbus_object_path__


class Storage(Repo, dbus.service.Object):
    """Storage based on a repository."""

    _config_default_values = { "prefetch": "true" }

    def __init__(self, bus, path):
        self.bus = bus
        self.remotes = {}
        self._boxes = {}
        self.config = Config(self, self._config_default_values)
        Repo.__init__(self, path)
        dbus.service.Object.__init__(self, bus,
                                     "%s/%s_%s" % (BUS_PATH,
                                                   dbus_clean_name(os.path.splitext(os.path.basename(path))[0]),
                                                   dbus_uuid()))

    @classmethod
    def _init_maybe_bare(cls, bus, path, bare):
        for d in BASE_DIRECTORIES:
            os.mkdir(os.path.join(path, *d))
        DiskObjectStore.init(os.path.join(path, OBJECTDIR))
        ret = cls(bus, path)
        ret.refs.set_symbolic_ref("HEAD", "refs/heads/master")
        ret._init_files(bare)
        return ret

    @classmethod
    def init_bare(cls, bus, path):
        return cls._init_maybe_bare(bus, path, True)

    def _push_determine_wants(self, refs):
        """Determine wants for a remote having refs.
        Return a dict { ref: sha } used to update the remote when pushing."""
        # Only push if the remote has already the branch
        # Find common branches
        common = set(refs.keys()) & set(self._boxes.keys())
        # XXX If remote has prefetch, push the blob of
        # refs[box_name]..self._boxes[box_name].head
        # …if the push has been successful
        # (would be stupid to push blobs is branch are totally different)
        return dict([ (box_name, self._boxes[box_name].head) for box_name in common ])

    def push(self):
        """Push boxes and its blobs/tags to remotes."""
        for remote in self.iterremotes():
            try:
                remote.push(self.determine_wants)
            except UpdateRefsError:
                # XXX We should probably fetch in such a case.
                pass

    def __getitem__(self, key):
        try:
            return super(Storage, self).__getitem__(key)
        except KeyError:
            # SHA1 not found, try to fetch it
            return super(Storage, self).__getitem__(self._fetch_sha1(key))

    def iterremotes(self):
        """Iterate over remotes, honoring weight."""
        for weight in sorted(self.remotes.keys()):
            yield self.remotes[weight]

    def _fetch_sha1(self, sha1):
        for remote in self.iterremotes():
            # Try to fetch and return the sha1 if ok
            try:
                remote.fetch_sha1s([ sha1 ])
                return sha1
            # If fetch failed, continue to next remote
            except FetchError:
                pass
        # We were unable to fetch
        raise FetchError

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         in_signature='s', out_signature='o')
    def GetBox(self, name):
        keyname = "refs/heads/%s" % name
        try:
            return self._boxes[keyname].__dbus_object_path__
        except KeyError:
            self._boxes[keyname] = Box(self, name)
        return self._boxes[keyname].__dbus_object_path__

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         in_signature='si', out_signature='o')
    def AddRemote(self, url, weight):
        if url not in [ r.url for r in self.remotes.values() ]:
            while self.remotes.has_key(weight):
                weight += 1
            self.remotes[weight] = Remote(self, url)
        return self.remotes[weight].__dbus_object_path__

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         in_signature='o')
    def RemoveRemote(self, object_path):
        for k, r in self.remotes.iteritems():
            if r.__dbus_object_path__ == object_path:
                del self.remotes[k]
                break

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         out_signature='a(oi)')
    def ListRemotes(self):
        return [ (r.__dbus_object_path__, w) for w, r in self.remotes.iteritems() ]

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         out_signature='s')
    def GetPath(self):
        return self.path

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         in_signature='ss')
    def SetConfig(self, key, value):
        if key in self._config_default_values.keys():
            self.config[key]= str(value)

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         in_signature='s', out_signature='s')
    def GetConfig(self, key):
        try:
            return self.config[key]
        except KeyError:
            return ''

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         out_signature='as')
    def ListConfigKeys(self):
        return self._config_default_values.keys()


class Box(threading.Thread, dbus.service.Object):

    def __init__(self, storage, name):
        self._next_record_lock = threading.Lock()
        self.config = {}
        self.storage = storage
        self.box_name = name
        # The next record
        self._next_record = None
        dbus.service.Object.__init__(self, storage.bus,
                                     "%s/%s" % (storage.__dbus_object_path__, name))
        threading.Thread.__init__(self, name="Box %s on Storage %s" % (name, storage.path))
        self.daemon = True

    @property
    def root(self):
        """Return the root directory."""
        return self.record.root

    @property
    def is_writable(self):
        return True

    @property
    def record(self):
        """Return current record. Default is to return a copy of the current
        commit so it can be modified, or a new commit if no commit exist."""

        with self._next_record_lock:
            if self._next_record is None:
                # Try to copy the current head
                try:
                    self._next_record = Record(self.storage, self.storage[self.head])
                # Store parent now, for comparison in commit()
                    self._next_record.parents = [ self.head ]
                except KeyError:
                    # Create a record based on brand new commit!
                    self._next_record = Record(self.storage, None)

            return self._next_record

    @property
    def head(self):
        return self.storage.refs["refs/heads/%s" % self.box_name]

    @head.setter
    def head(self, value):
        self.storage.refs['refs/heads/%s' % self.box_name] = value
        self.Commited()

    @dbus.service.method(dbus_interface="%s.Box" % BUS_INTERFACE)
    def Commit(self):
        """Commit modification to the storage, if needed."""
        with self._next_record_lock:
            if self._next_record is not None:
                # XXX We may need to lock _next_record
                # and have a global object lock in Repo
                new_root_id = self._next_record.root.id()
                # Check that there's changes in the next record by comparing its
                # root tree id against its parent's root tree id, or by checking
                # if it's a merge record, or the first one. But anyhow, its new
                # root must be different than head's current, otherwise we
                # would have nothing to do and could drop that next record.
                # if is_merge_commit \
                #     or (first_commit \
                #         or next_record_has_modification) \
                #         and next_record_is_different_than_head)
                print "New root tree id: ",
                print new_root_id
                if len(self._next_record.parents) == 0 \
                        or ((len(self._next_record.parents) > 1 \
                                 or new_root_id != self.storage[self._next_record.parents[0]].tree) \
                                and new_root_id != self.storage[self.head].tree):
                    # We have a different root tree, so we are different. Hehe.
                    print " Next record root tree is different"
                    try:
                        head = self.head
                    except KeyError:
                        head = None
                    # if first_commit or fast_forward
                    if (head is None and len(self._next_record.parents) == 0) \
                            or (head is not None and head in self._next_record.parents):
                        # Current head is still one of our parents, so no
                        # problem updating head. This is a fast-forward.
                        print "  Doing a fast-forward"
                        self.head = self._next_record.store()
                    else:
                        # Hum, current head is not our parent. It changed. We
                        # need to merge head in the next record to create a
                        # merge commit.
                        print "  Merging head into next record"
                        self._next_record.merge_commit(head)
                        if not self.refs.set_if_equals("refs/heads/%s" % self.box_name,
                                                       head,
                                                       self._next_record.store()):
                            # head changed while we were doing our merge, so
                            # retry to commit once again, merging this new
                            # head.
                            print "  head changed, holy shit, recommit!"
                            return self.Commit()
                # If _next_record did not change (no root tree change), we just
                # reset in case head would have changed under our feet while
                # we were away.
                # If _next_record changed, we still reset it to make a new one
                # as soon as someone will need.
                self._next_record = None

    @dbus.service.signal(dbus_interface="%s.Box" % BUS_INTERFACE)
    def Commited(self):
        self.storage.push()

    def run(self):
        from .fs import NodlehsFuse
        TimeCommiter(self, 300).start()
        FUSE(NodlehsFuse(self), self.mountpoint, debug=True)
        self.Commit()

    @dbus.service.method(dbus_interface="%s.Box" % BUS_INTERFACE,
                         in_signature='s')
    def Mount(self, mountpoint):
        if not self.is_alive():
            self.mountpoint = mountpoint
            self.start()
