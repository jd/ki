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

    # This is locally modified/added files which will belong to our tree
    # when we will dump ourselves.
    local_tree = {}

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
        # If child is empty, we are asked for ourselves.
        # This happens if the absolute path asked is /
        # Yeah, that means we are the r00t directory, indeed.
        if name is '':
            return (stat.S_IFDIR, self)

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
        components = Path(path).components
        (mode, child) = self._child_from_name(components[0])

        if len(components) == 1:
            return (mode, child)

        if isinstance(child, Directory):
            return child.child(Path(components[1:]))

        raise NotDirectory(child)

    def add(self, name, mode, f):
        """Add a file with name and mode attributes to directory."""
        self.local_tree[name] = (mode, f)


class File(Storable):
    """A file."""

    def __str__(self):
        return self.object.__str__()


class Record(Storable):
    """A commit record."""

    _root = None

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
        self.object.author_time = \
            self.object.commit_time = \
            int(time.time())
        # Store root
        self.root.store()
        # Store the ref to the root tree
        self.object.tree = self.root.id
        # Store us
        super(Record, self).store()


class ReadOnly(Exception):
    """Read only exception."""
    pass


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

            # XXX move this away in Record? We should not access object directly.
            self._next_record.object.author = pwd.getpwuid(os.getuid()).pw_gecos.split(",")[0]
            self._next_record.object.committer = "Nodlehs"
            self._next_record.object.message = "Nodlehs auto-commit"
            # XXX Set a real TZ
            self._next_record.object.author_timezone = \
                self._next_record.object.commit_timezone = \
                parse_timezone('+0200')[0]

        return self._next_record

    @next_record.setter
    def next_record(self, value):
        self._next_record = None

    def commit(self):
        """Commit modification to the storage."""
        self.next_record.store()
        self.next_record = None
        # XXX update head

    def add_file(self, path, mode):
        """Add a file to the repository."""
        if not self.is_writable():
            raise ReadOnly

        path = Path(path)
        # Get the directory which will own the file
        (directory_mode, directory) = self.next_record.root.child(path[:-1])
        # Add the file
        directory.add(path[-1], mode, File(self, Blob()))
