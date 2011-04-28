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
from dulwich.repo import Repo
from dulwich.client import UpdateRefsError
import os
import uuid
import threading
import dbus.service

class Storage(Repo, threading.Thread, dbus.service.Object):
    """Storage based on a repository."""

    def __init__(self, root, mountpoint, bus):
        self.current_record_override = None
        # The next record
        self._next_record = None
        self.remotes = OrderedSet()
        self.mountpoint = mountpoint
        threading.Thread.__init__(self, name="Thread Storage %s" % root)
        Repo.__init__(self, root)
        self.daemon = True

        # Build a name
        dbus.service.Object.__init__(self, bus,
                                     "%s/%s_%s" % (BUS_PATH,
                                                   filter(lambda x: 'a' <= x <= 'z' or 'A' <= x <= 'Z' or '0' <= x <= '9' or x == '_' ,
                                                          os.path.splitext(os.path.basename(root))[0]),
                                                   "".join(map(lambda x: x == '-' and '_' or x, str(uuid.uuid4())))))

    def is_writable(self):
        """Check that the storage is writable."""
        return self.current_record_override is None

    @property
    def root(self):
        """Return the root directory."""
        return self.head().root

    @property
    def master(self):
        return self.refs["refs/heads/master"]

    @master.setter
    def master(self, value):
        self.refs['refs/heads/master'] = value

    def head(self):
        """Return current head. Default is to return a copy of the current
        head so it can be modified, or a new commit if no commit exist."""

        if self.current_record_override is not None:
            return self.current_record_override

        if self._next_record is None:
            # Try to copy the current head
            try:
                self._next_record = Record(self, self[self.master])
                # Store parent now, for comparison in commit()
                self._next_record.parents = [ self.master ]
            except KeyError:
                # Create a record based on brand new commit!
                self._next_record = Record(self, None)

        return self._next_record

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

    def push(self):
        """Push master and all tags to remotes."""
        # XXX Do not push every objects, respect what the remote wants.
        #     This could be done by fetching refs/tags/config on the remote, which
        #     would be a blob with the configuration file or the configuration object
        #     we could pickle. :)
        refs = dict([ ("refs/tags/%s" % tag, ref)
                      for tag, ref in self.refs.as_dict("refs/tags").iteritems() ])
        refs["refs/heads/master"] = self.refs['refs/heads/master']
        # XXX Make that threaded?
        for remote in self.remotes:
            try:
                remote.push(refs)
            except UpdateRefsError:
                # XXX We should probably fetch in such a case.
                pass

    def commit_and_push(self):
        self.commit()
        self.push()

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

    def run(self):
        from .fs import NodlehsFuse
        FUSE(NodlehsFuse(self), self.mountpoint, debug=True)

    @dbus.service.method(dbus_interface=BUS_INTERFACE)
    def Mount(self):
        if not self.is_alive():
            self.start()
