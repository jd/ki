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
from .constants import *
from .objects import Record
from dulwich.repo import Repo, BASE_DIRECTORIES, OBJECTDIR, DiskObjectStore
from dulwich.client import UpdateRefsError
import os
import uuid
import threading
import dbus.service


class Storage(Repo, dbus.service.Object):
    """Storage based on a repository."""

    def __init__(self, bus, root):
        self.bus = bus
        self.remotes = OrderedSet()
        self._branches = {}
        Repo.__init__(self, root)
        # Build a name
        dbus.service.Object.__init__(self, bus,
                                     "%s/%s_%s" % (BUS_PATH,
                                                   filter(lambda x: 'a' <= x <= 'z' or 'A' <= x <= 'Z' or '0' <= x <= '9' or x == '_' ,
                                                          os.path.splitext(os.path.basename(root))[0]),
                                                   "".join(map(lambda x: x == '-' and '_' or x, str(uuid.uuid4())))))

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
        """Push all branches and all tags to remotes."""
        # XXX Do not push every objects, respect what the remote wants.
        #     This could be done by fetching refs/tags/config on the remote, which
        #     would be a blob with the configuration file or the configuration object
        #     we could pickle. :)
        refs = dict([ ("refs/tags/%s" % tag, ref)
                      for tag, ref in self.refs.as_dict("refs/tags").iteritems() ])
        refs.update(dict([ ("refs/heads/%s" % head, ref)
                      for tag, ref in self.refs.as_dict("refs/heads").iteritems() ]))
        # XXX Make that threaded?
        for remote in self.remotes:
            try:
                remote.push(refs)
            except UpdateRefsError:
                # XXX We should probably fetch in such a case.
                pass

    def __getitem__(self, key):
        try:
            return super(Storage, self).__getitem__(key)
        except KeyError:
            # SHA1 not found, try to fetch it
            return super(Storage, self).__getitem__(self._fetch_sha1(key))

    def _fetch_sha1(self, sha1):
        for remote in self.remotes:
            # Try to fetch and return the sha1 if ok
            try:
                remote.fetch_sha1s([ sha1 ])
                return sha1
            # If fetch failed, continue to next remote
            except FetchError:
                pass
        # We were unable to fetch
        raise FetchError

    @dbus.service.method(dbus_interface=BUS_INTERFACE,
                         in_signature='s', out_signature='o')
    def get_branch(self, name):
        try:
            return self._branches[name]
        except KeyError:
            self._branches[name] = Branch(self, name)
        return self._branches[name]



class Branch(dbus.service.Object, threading.Thread):

    def __init__(self, storage, name):
        self.storage = storage
        self.branch_name = name
        self.current_record_override = None
        # The next record
        self._next_record = None
        dbus.service.Object.__init__(self, storage.bus,
                                     "%s/%s" % (storage.__dbus_object_path__, name))
        threading.Thread.__init__(self, name="Branch %s on Storage %s" % (name, storage.path))
        self.daemon = True

    @property
    def root(self):
        """Return the root directory."""
        return self.record.root

    def is_writable(self):
        """Check that the storage is writable."""
        return self.current_record_override is None

    @property
    def record(self):
        """Return current record. Default is to return a copy of the current
        commit so it can be modified, or a new commit if no commit exist."""

        if self.current_record_override is not None:
            return self.current_record_override

        if self._next_record is None:
            # Try to copy the current head
            try:
                self._next_record = Record(self, self.storage[self.head])
                # Store parent now, for comparison in commit()
                self._next_record.parents = [ self.master ]
            except KeyError:
                # Create a record based on brand new commit!
                self._next_record = Record(self, None)

        return self._next_record

    @property
    def head(self):
        return self.storage.refs["refs/heads/%s" % self.branch_name]

    @head.setter
    def head(self, value):
        self.storage.refs['refs/heads/%s' % self.branch_name] = value

    def commit(self):
        """Commit modification to the storage, if needed."""
        if self._next_record is not None:
            # XXX We may need to lock _next_record
            # and have a global object lock in Repo
            new_root_id = self._next_record.root.id()
            # Check that there's changes in the next record by comparing its
            # root tree id against its parent's root tree id, or by checking
            # if it's a merge record. But anyhow, its new root must be
            # different than master's current, otherwise we would have
            # nothing to do and could drop that next record.
            # if (is_merge_commit or next_record_has_modification) \
            #     and next_record_is_different_than_master
            print "New root tree id: ",
            print new_root_id
            if (len(self._next_record.parents) > 1 \
                    or new_root_id != self[self._next_record.parents[0]].tree) \
                    and new_root_id != self[self.master].tree:
                # We have a different root tree, so we are different. Hehe.
                print " Next record root tree is different"
                if self.master in self._next_record.parents:
                    # Current master is still one of our parents, so no
                    # problem updating master. This is a fast-forward.
                    print "  Doing a fast-forward"
                    self.master = self._next_record.store()
                else:
                    # Hum, current master is not our parent. It changed. We
                    # need to merge master in the next record to create a
                    # merge commit.
                    print "  Merging master into next record"
                    # Store master in a variable to be sure not to retrieve
                    # it from the repo twice!
                    master = self.master
                    self._next_record.merge_commit(master)
                    if not self.refs.set_if_equals("refs/heads/master",
                                                   master,
                                                   self._next_record.store()):
                        # Master changed while we were doing our merge, so
                        # retry to commit once again, merging this new
                        # master.
                        print "  Master changed, holy shit, recommit!"
                        return self.commit()
            # If _next_record did not change (no root tree change), we just
            # reset in case master would have changed under our feet while
            # we were away.
            # If _next_record changed, we still reset it to make a new one
            # as soon as someone will need.
            self._next_record = None

    def run(self):
        from .fs import NodlehsFuse
        FUSE(NodlehsFuse(self), self.mountpoint, debug=True)

    @dbus.service.method(dbus_interface=BUS_INTERFACE,
                         in_signature='s')
    def Mount(self, mountpoint):
        if not self.is_alive():
            self.mountpoint = mountpoint
            self.start()
