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
from .objects import Record, FileBlock, FetchError
from .remote import Remote, Syncer
from .commiter import TimeCommiter
from .fs import NodlehsFuse
from dulwich.repo import Repo, BASE_DIRECTORIES, OBJECTDIR, DiskObjectStore
from dulwich.client import UpdateRefsError
from dulwich.objects import Commit, Blob
from dulwich.errors import HangupException
import os
import uuid
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

    def __init__(self, bus):
        # XXX Singleton?
        self.storages = {}
        self.user_storage = None
        self.bus = bus
        super(StorageManager, self).__init__(bus, "%s/%s" % (BUS_PATH,
                                                             self.__class__.__name__))

    def create_storage(self, path=None):
        if path is None:
            path = xdg.BaseDirectory.save_data_path("nodlehs/storage")
        if len(os.listdir(path)) is 0:
            return Storage.init_bare(self.bus, path)
        s = Storage(self.bus, path)
        s.syncer.start()
        return s

    @dbus.service.method(dbus_interface="%s.StorageManager" % BUS_INTERFACE,
                         in_signature='s', out_signature='o')
    def GetStorage(self, path):
        """Create a storage."""
        path = os.path.abspath(path)
        if not self.storages.has_key(path):
            self.storages[path] = self.create_storage(path)
        return self.storages[path].__dbus_object_path__

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
        self.must_be_sync = threading.Event()
        Repo.__init__(self, path)
        dbus.service.Object.__init__(self, bus,
                                     "%s/%s_%s" % (BUS_PATH,
                                                   dbus_clean_name(os.path.splitext(os.path.basename(path))[0]),
                                                   dbus_uuid()))
        self.syncer = Syncer(self)

    @property
    def id(self):
        try:
            return str(FileBlock(self, self.refs[Remote._id_ref]))
        except KeyError:
            f = FileBlock(self)
            f.data = str(uuid.uuid4())
            self.refs[Remote._id_ref] = f.store()
            return str(f)

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

    def _fetch_determine_refs(self, refs):
        """Determine what should be fetched based on refs.
        This return the list of refs matching storages except self."""
        return dict([ (ref, sha) for ref, sha in refs.iteritems()
                      if ref.startswith("refs/storages/") and not ref.startswith("refs/storages/%s" % self.id) ])
    @staticmethod
    def blobs_list_dict(blobs):
        """Return a dict mapping of every blob to its ref.
        That means that the returned dict will have the format

          { "refs/blobs/<blob>": "<blob>" }"""
        return dict([ ("refs/blobs/%s" % blob, blob) for blob in blobs ])

    def push(self):
        """Push all boxes to all remotes."""
        for remote in self.iterremotes():
            def determine_wants(oldrefs):
                """Determine wants for a remote having refs.
                Return a dict { ref: sha } used to update the remote when pushing."""
                newrefs = oldrefs.copy()

                for branch_name, head in self.refs.as_dict("refs/storages").iteritems():
                    # Do NOT push the storage stuff it's the remote ones!
                    if branch_name.split('/', 1)[0] != remote.id:
                        newrefs["refs/storages/%s" % branch_name] = head
                        # XXX implements and use history(Ndays)
                        newrefs.update(self.blobs_list_dict(filter(lambda blob:
                                                                       self.refs.as_dict("refs/blobs").has_key(blob),
                                                                   Record(self, head).determine_blobs())))
                return newrefs

            try:
                remote.push(determine_wants)
            except UpdateRefsError as e:
                print "> Update ref error"
                print e.ref_status

    def fetch(self):
        """Fetch all boxes from all remotes."""
        for remote in self.iterremotes():
            refs = self._fetch_determine_refs(remote.fetch(lambda refs:
                                                               self._fetch_determine_refs(refs).values()))
            for ref, sha in refs.iteritems():
                # Store fetched refs: refs["refs/storages/REMOTE_ID/…"] = sha
                self.refs[ref] = sha

    def fetch_blobs(self):
        """Fetch all needed blobs."""
        for head in self.refs.as_dict("refs/storages").itervalues():
            for blob in self.blobs_list_dict(Record(self, head).determine_blobs()).itervalues():
                self[blob]

    def update_from_remotes(self):
        for box in self._boxes.itervalues():
            box.update_from_remotes()

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
                print "> Trying to fetch %s on remote %s" % (sha1, remote)
                remote.fetch_sha1s([ sha1 ])
                if isinstance(self[sha1], Blob):
                    self.refs["refs/blobs/%s" % sha1] = sha1
                return sha1
            # If fetch failed, continue to next remote
            except HangupException:
                pass
        # We were unable to fetch
        raise FetchError(sha1)

    def get_box(self, name, create=False):
        try:
            return self._boxes[name]
        except KeyError:
            self._boxes[name] = Box(self, name, create)
        return self._boxes[name]

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         in_signature='s', out_signature='o')
    def CreateBox(self, name):
        # XXX validate name based on git tech spec
        return self.get_box("%s_%s" % (name, dbus_uuid()), True).__dbus_object_path__

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         in_signature='s', out_signature='o')
    def GetBox(self, name):
        return self.get_box(name).__dbus_object_path__

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         out_signature='as')
    def ListBoxes(self):
        return set([ ref.split('/', 1)[1] for ref in self.refs.as_dict("refs/storages").keys() ])

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

    @dbus.service.method(dbus_interface="%s.Storage" % BUS_INTERFACE,
                         out_signature='s')
    def GetID(self):
        return self.id


class NotFastForward(Exception):
    pass


class NoPlutoniumInDeLoreanError(Exception):
    """You cannot go back in time."""
    pass


class NoRecord(Exception):
    pass


class Box(threading.Thread, dbus.service.Object):

    def __init__(self, storage, name, create=False):
        self.head_lock = threading.RLock()
        self.config = {}
        self.storage = storage
        self.box_name = name
        # The next record
        self._next_record = None
        dbus.service.Object.__init__(self, storage.bus,
                                     "%s/%s" % (storage.__dbus_object_path__, name))
        threading.Thread.__init__(self, name="Box %s on Storage %s" % (name, storage.path))
        self.daemon = True
        self.fuse = NodlehsFuse(self)
        if create:
            self.head = Record(self.storage)
        else:
            self.update_from_remotes()

    @property
    def root(self):
        """Return the root directory."""
        return self.record.root

    @property
    def is_writable(self):
        return True

    def more_recent_record_on_remotes(self):
        """Return the more recent commit for a box on mirrored remotes."""
        try:
            return max([ Record(self.storage, sha) \
                             for box, sha in self.storage.refs.as_dict("refs/storages").iteritems() \
                             if box.split('/', 1)[1] == self.box_name ])
        except ValueError:
            raise NoRecord

    def update_from_remotes(self):
        try:
            print "> Update from remote…"
            print self
            self.head = self.more_recent_record_on_remotes()
        except NoPlutoniumInDeLoreanError:
            print " > Denied: not going back in time doc"
        except NotFastForward:
            print " > Denied: UNRELATED ?!"

    @property
    def record(self):
        """Return current record. Default is to return a copy of the current
        commit so it can be modified, or a new commit if no commit exist."""
        with self.head_lock:
            if self._next_record is None:
                # Copy the current head
                self._next_record = self.head
                # Store parent now, for comparison in commit()
                self._next_record.parents.clear()
                self._next_record.parents.append(self.head)
            return self._next_record

    @property
    def head(self):
        try:
            return Record(self.storage, self.storage.refs["refs/storages/%s/%s" % (self.storage.id, self.box_name)])
        except KeyError:
            raise NoRecord

    @head.setter
    def head(self, value):
        if isinstance(value, str):
            value = self.storage[value]
        if isinstance(value, Commit):
            value = Record(self.storage, value)
        with self.head_lock:
            try:
                head = self.head
            except NoRecord:
                self.storage.refs["refs/storages/%s/%s" % (self.storage.id, self.box_name)] = value.store()
            else:
                # Nothing to do (the easy case)
                if head == value:
                    pass
                elif value.is_child_of(head):
                    # If it's a child, it's ok
                    self.storage.refs["refs/storages/%s/%s" % (self.storage.id, self.box_name)] = value.store()
                    self.fuse.fds.reset()
                elif head.is_child_of(value):
                    # Trying to go back in time?
                    raise NoPlutoniumInDeLoreanError
                elif head.find_common_ancestors(value):
                    # They got common ancestors, so they are from the same repo at
                    # least. We can merge them.
                    # Copy head into a new record
                    merge_record = head.copy()
                    merge_record.parents.clear()
                    merge_record.parents.append(head)
                    merge_record.merge_commit(value)
                    self.storage.refs["refs/storages/%s/%s" % (self.storage.id, self.box_name)] = merge_record.store()
                    self.fuse.fds.reset()
                else:
                    # This is only raised if they got not common ancestor, so
                    # they are totally unrelated. This is abnormal.
                    raise NotFastForward

    @dbus.service.method(dbus_interface="%s.Box" % BUS_INTERFACE)
    def Commit(self):
        """Commit modification to the storage, if needed."""
        with self.head_lock:
            if self._next_record is not None:
                print "The next record root tree id: ",
                print self._next_record.root.id()
                # Check that there's changes in that next record by
                # comparing its root tree id against head's one and its
                # parents one. If it's not different that these ones,
                # committing is useless.
                #
                # if commit's tree is different than current head's one \
                #         and different than its parents ones
                if self._next_record.root != self.head.root \
                        and self._next_record.root not in [ p.root for p in self._next_record.parents ]:
                    print " Next record root tree is different"
                    self._next_record.update_timestamp()
                    self.head = self._next_record
                    self.Commited()
                # If _next_record did not change (no root tree change), we just
                # reset in case head would have changed under our feet while
                # we were away.
                # If _next_record changed, we still reset it to make a new one
                # as soon as someone will need.
                self._next_record = None

    @dbus.service.signal(dbus_interface="%s.Box" % BUS_INTERFACE)
    def Commited(self):
        self.storage.must_be_sync.set()

    def run(self):
        TimeCommiter(self, 300).start()
        FUSE(self.fuse, self.mountpoint, nothreads=True, debug=True)
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
