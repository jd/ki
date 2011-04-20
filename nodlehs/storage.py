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

from .utils import Path
from dulwich.repo import Repo
from dulwich.objects import Blob, Commit, Tree, parse_timezone
from StringIO import StringIO
import stat
import time
import pwd
import os


class NoRecord(Exception):
    """This storage has no record."""
    pass


class UnknownObjectType(Exception):
    """This object is unknown."""
    pass


class NoChild(Exception):
    """There is no such child."""
    pass


class NotDirectory(Exception):
    """This is not a directory."""
    pass


class Storable(object):

    # The internal object
    object = None

    def __init__(self, storage, obj):
        self.storage = storage
        self.object = obj

    @property
    def id(self):
        return self.object.id

    def store(self):
        """Store object into its storage."""
        self.storage.object_store.add_object(self.object)

    def __len__(self):
        return self.object.raw_length()

    def __repr__(self):
        return "<" + self.__class__.__name__ + " " + hex(id(self)) + " for " + self.object.id + ">"

class Directory(Storable):
    """A directory."""

    def __init__(self, storage, obj):
        # This is locally modified/added files which will belong to our tree
        # when we will dump ourselves.
        self.local_tree = {}
        super(Directory, self).__init__(storage, obj)

    def store(self):
        for name, info in self.local_tree.iteritems():
            (mode, child) = info
            self.object.add(name, mode, child.id)
            child.store()
        super(Directory, self).store()

    def __iter__(self):
        for name, entry in self.local_tree.iteritems():
            yield (entry[0], name)
        for entry in self.object.iteritems():
            yield (entry.mode, entry.path)

    def _child_from_name(self, name):
        """Get a child of the directory by its name."""
        # First try to get the (mode, child) from the local tree
        try:
            return self.local_tree[name]
        except KeyError:
            try:
                (mode, child_sha) = self.object[name]
                child = self.storage[child_sha]
            except KeyError:
                raise NoChild(name)
        if isinstance(child, Blob):
            ret = (mode, File(self.storage, child))
        elif isinstance(child, Tree):
            ret = (mode, Directory(self.storage, child))

        # Store returned file in local_tree: if it is ever modified, we will
        # be able to store its new content (new blob id) automagically.
        if ret:
            local_tree[name] = ret
            return ret

        raise UnknownObjectType(child)

    def child(self, path):
        """Get the child of that directory that is at path."""
        # If len(path) is 0, we are asked for ourselves.
        # This happens if the absolute path asked is /
        # Yeah, that means we are the r00t directory, indeed.

        path = Path(path)

        if len(path) == 0:
            return (stat.S_IFDIR, self)

        (mode, child) = self._child_from_name(path[0])

        if len(path) == 1:
            return (mode, child)

        if isinstance(child, Directory):
            return child.child(path[1:])

        raise NotDirectory(child)

    def add(self, name, mode, f):
        """Add a file with name and mode attributes to directory."""
        self.local_tree[name] = (mode, f)
        self.mtime = time.time()

    def remove(self, name):
        """Remove a file with name from directory."""
        try:
            # Try to delete in the local tree
            del self.local_tree[name]
        except KeyError:
            # The file was not in local_tree, try in self.object and raises
            # if it raises.
            try:
                del self.object[name]
            except KeyError:
                raise NoChild(name)
        else:
            # We succeeded to delete in local_tree, just try to delete in
            # self.object to be sure we deleted definitively.
            try:
                del self.object[name]
            except KeyError:
                pass

        self.mtime = time.time()


class File(Storable):
    """A file."""

    def __init__(self, storage, obj):
        self._data = StringIO(obj.data)
        super(File, self).__init__(storage, obj)

    def __len__(self):
        return len(self._data.getvalue())

    def __str__(self):
        return self._data.getvalue()

    def seek(self, offset):
        return self._data.seek(offset)

    def read(self, value):
        return self._data.read(value)

    def write(self, data):
        self._data.write(data)
        self.mtime = time.time()
        return len(data)

    def truncate(self, size=None):
        self._data.truncate(size)
        self.mtime = time.time()

    def store(self):
        # Update object data
        self.object.set_raw_string(self._data.getvalue())
        # Store
        super(Record, self).store()


class Symlink(File):
    """A symlink."""

    def __init__(self, storage, obj, target):
        super(Symlink, self).__init__(storage, obj)
        self.write(target)

    @property
    def target(self):
        return self._data.getvalue()

    @target.setter
    def target(self, value):
        self._data.truncate(0)
        self._data.write(value)


class Record(Storable):
    """A commit record."""

    _root = None

    def __init__(self, storage, commit):
        super(Record, self).__init__(storage, commit)
        self.object.author = pwd.getpwuid(os.getuid()).pw_gecos.split(",")[0]
        self.object.committer = "Nodlehs"
        self.object.message = "Nodlehs auto-commit"
        self.object.author_timezone = \
            self.object.commit_timezone = \
            - time.timezone

    @property
    def root(self):
        """The root directory associated with the record."""
        if self._root is None:
            try:
                # Initialize with the commit tree
                self._root = Directory(self.storage, self.storage[self.object.tree])
            except AttributeError:
                # Initialize with a new tree
                self._root = Directory(self.storage, Tree())
        return self._root

    @root.setter
    def root(self, value):
        self._root = value

    def store(self):
        # Update time
        # XXX maybe checking for root tree items mtime would be better and
        # more accurate?
        self.object.author_time = \
            self.object.commit_time = \
            int(time.time())
        # Store root
        self.root.store()
        # Store the ref to the root tree
        self.object.tree = self.root.id
        # Store us
        super(Record, self).store()


class Storage(Repo):
    """Storage based on a repository."""

    current_record_override = None
    # The next record
    _next_record = None

    @property
    def current_record(self):
        """The current storage record state.

        This is the storage state the user wants to see. This returns either
        the next record which is being prepared, or the overriding record if
        we have been asked to go somewhere else in time, or the storage
        head."""
        return self.next_record if self.next_record is not None \
            else self.current_record_override if self.current_record_override is not None \
            else self.head

    @property
    def head(self):
        """Return the real storage head.

        This is the real storage head, the currently recorded state.
        This raise NoRecord if there is no record in the storage."""
        try:
            return Record(self, self[super(Storage, self).head()])
        except KeyError:
            raise NoRecord

    def is_writable(self):
        """Check that the storage is writable."""
        return self.current_record_override is None

    @property
    def root(self):
        """Return the root directory."""
        try:
            return self.current_record.root
        except NoRecord:
            return self.next_record.root

    @property
    def next_record(self):
        if self._next_record is None:
            # Try to copy the current head
            try:
                self._next_record = self.head
                self._next_record.object.parents = [ self.head.object.id ]
            except NoRecord:
                # Create a record based on brand new commit!
                self._next_record = Record(self, Commit())

        return self._next_record

    @next_record.setter
    def next_record(self, value):
        self._next_record = None

    def commit(self):
        """Commit modification to the storage."""
        self.next_record.store()
        self.next_record = None
        # XXX update head
