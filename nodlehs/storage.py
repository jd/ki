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
from .config import Configurable, Config, BUS_INTERFACE
from .objects import Record
from .remote import Remote, FetchError
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


class Storage(Repo, dbus.service.Object, Configurable):
    """Storage based on a repository."""

    def __init__(self, bus, path):
        self.bus = bus
        self.remotes = {}
        self._boxes = {}
        Repo.__init__(self, path)
        dbus.service.Object.__init__(self, bus,
                                     "%s/%s_%s" % (BUS_PATH,
                                                   dbus_clean_name(os.path.splitext(os.path.basename(path))[0]),
                                                   dbus_uuid()))

    @property
    def config(self):
        try:
            config_blob = self[self.refs[Config.ref]]
        except KeyError:
            # No config
            config_blob = None
        return Config(self, self.on_config_store, config_blob)

    def on_config_store(self, sha):
        """Function called when the configuration is modified and stored on
        disk."""
        self.refs[Config.ref] = sha

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

    def push(self):
        """Push boxes and its blobs/tags to remotes."""
        for remote in self.iterremotes():
            # XXX If remote has prefetch, push the blob of
            # refs[box_name]..self._boxes[box_name].head
            # …if the push has been successful
            # (would be stupid to push blobs is branch are totally different)
            def determine_wants(oldrefs):
                """Determine wants for a remote having refs.
                Return a dict { ref: sha } used to update the remote when pushing."""
                try:
                    wanted_boxes = set(self._boxes.keys()) & set(remote.config["boxes"].keys())
                except KeyError:
                    return {}
                newrefs = {}
                for box_name in wanted_boxes:
                    branch_name = "refs/heads/%s" % box_name
                    head = self._boxes[box_name].head
                    # Update box to its current head
                    newrefs[branch_name] = head
                    # Check that box is configured for prefetch on the remote
                    try:
                        prefetch = remote.config["boxes"][box_name]["prefetch"]
                    except KeyError:
                        prefetch = False
                    if prefetch:
                        head_record = Record(self, self[head])
                        if oldrefs.has_key(branch_name):
                            # Find the list of missing records between the remote and ourself
                            missing_records = head_record.commit_intervals(Record(self, oldrefs[branch_name]))
                        else:
                            # The remote never had this branch, all records are missing
                            missing_records = reduce(set.union, head_record.commit_history_list())
                        # Build the blob set list of all missing commits
                        blobs = set()
                        for record in missing_records:
                            blobs.update(record.root.list_blobs_recursive())
                        # Ask to send every blob of every missing commits
                        newrefs.update([ ("refs/tags/%s" % blob, blob) for blob in blobs ])
                print "RETURNING NEWREFS"
                print newrefs
                return newrefs

            try:
                remote.push(determine_wants)
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
        for remote in sorted(self.remotes.values()):
            yield remote

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
        try:
            return self._boxes[name].__dbus_object_path__
        except KeyError:
            self._boxes[name] = Box(self, name)
        return self._boxes[name].__dbus_object_path__

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         in_signature='ssi', out_signature='o')
    def AddRemote(self, name, url, weight):
        self.remotes[name] = Remote(self, name, url, weight)
        return self.remotes[name].__dbus_object_path__

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         in_signature='s')
    def RemoveRemote(self, name):
        try:
            del self.remotes[name]
        except KeyError:
            pass

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         out_signature='ao')
    def ListRemotes(self):
        return [ r.__dbus_object_path__ for r in  self.iterremotes() ]

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         out_signature='s')
    def GetPath(self):
        return self.path


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
                try:
                    # Try to copy the current head
                    self._next_record = Record(self.storage, self.storage[self.head])
                    # Store parent now, for comparison in commit()
                    self._next_record.parents = [ Record(self.storage, self.storage[self.head]) ]
                except KeyError:
                    # This can happen if self.head does not exists.
                    self._next_record = Record(self.storage)
            return self._next_record

    @property
    def head(self):
        return self.storage.refs["refs/heads/%s" % self.box_name]

    @dbus.service.method(dbus_interface="%s.Box" % BUS_INTERFACE)
    def Commit(self):
        """Commit modification to the storage, if needed."""
        with self._next_record_lock:
            if self._next_record is not None:
                # Check that there's changes in the next record by comparing
                # its root tree id against head's one, otherwise we would
                # have nothing to do and could drop that next record.
                print "New root tree id: ",
                print self._next_record.root.id()
                try:
                    head = self.head
                except KeyError:
                    head = None
                if head is None \
                        or self._next_record.root.id() != self.storage[head].tree:
                    # We have a different root tree, so we are different. Hehe.
                    print " Next record root tree is different"
                    # if first_commit or fast_forward
                    if head not in [ parent.id() for parent in self._next_record.parents ]:
                        # Hum, current head is not our parent. It changed. We
                        # need to merge head in the next record to create a
                        # merge commit.
                        print "  Merging head into next record"
                        self._next_record.merge_commit(head)
                    else:
                        print "  Doing a fast-forward"
                    if not self.storage.refs.set_if_equals("refs/heads/%s" % self.box_name,
                                                           head,
                                                           self._next_record.store()):
                        # head changed while we were doing our merge, so
                        # retry to commit once again, merging this new
                        # head.
                        print "  head changed, holy shit, recommit!"
                        return self.Commit()
                    else:
                        self.Commited()
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

    @dbus.service.method(dbus_interface="%s.Box" % BUS_INTERFACE,
                         out_signature='a(su)')
    def RecordList(self):
        """Return an array of struct containing (sha, commit time)."""
        return [ (commit.id(), commit.object.commit_time)
                 for commit_set in self.record.commit_history_list()
                 for commit in commit_set ]
