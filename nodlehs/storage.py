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

from .utils import *
from .remote import *
from dulwich.repo import Repo
from dulwich.objects import Blob, Commit, Tree, parse_timezone, S_IFGITLINK
from StringIO import StringIO
import stat
import time
import pwd
import os
import threading


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

    def __init__(self, storage, obj):
        self.storage = storage
        self.object = obj

    def store(self):
        """Store object into its storage.
        Return the object SHA1."""
        self.storage.object_store.add_object(self.object)
        return self.object.id

    def id(self):
        """Return the object SHA1 id.
        Note that this id can changed anytime, since data change all the time."""
        return self.object.id

    def __len__(self):
        return self.object.raw_length()

    def __repr__(self):
        return "<" + self.__class__.__name__ + " " + hex(id(self)) + " for " + self.object.id + ">"

def make_object(storage, sha):
    """Make a storage object from an sha."""
    item = storage[sha]
    if isinstance(item, Blob):
        return File(storage, item)
    if isinstance(item, Tree):
        return Directory(storage, item)
    raise UnknownObjectType(child)

class Directory(Storable):
    """A directory."""

    def __init__(self, storage, obj):
        # This is locally modified/added files which will belong to our tree
        # when we will dump ourselves.
        self.local_tree = {}
        super(Directory, self).__init__(storage, obj)

    def id(self):
        for name, (mode, child) in self.local_tree.iteritems():
            # We store file with the GITLINK property. This is an hack to be
            # sure git will send us the whole blob when we fetch the tree.
            if isinstance(child, File):
                mode |= S_IFGITLINK
            self.object.add(name, int(mode), child.id())
        return super(Directory, self).id()

    def store(self):
        for name, (mode, child) in self.local_tree.iteritems():
            # We store file with the GITLINK property. This is an hack to be
            # sure git will send us the whole blob when we fetch the tree.
            if isinstance(child, File):
                mode |= S_IFGITLINK
            self.object.add(name, int(mode), child.store())
        return super(Directory, self).store()

    def __iter__(self):
        entries = set(self.local_tree.keys())
        entries.update([ entry.path for entry in self.object.iteritems() ])
        for entry in entries:
            yield entry

    def _child_from_name(self, name):
        """Get a child of the directory by its name."""
        # First try to get the (mode, child) from the local tree
        try:
            return self.local_tree[name]
        except KeyError:
            try:
                (mode, child_sha) = self.object[name]
            except KeyError:
                raise NoChild(name)
            self.local_tree[name] = (mode, make_object(self.storage, child_sha))

        return self.local_tree[name]

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

    def rename(self, old, new):
        old = Path(old)
        new = Path(new)

        (old_directory_mode, old_directory) = self.child(old[:-1])
        (new_directory_mode, new_directory) = self.child(new[:-1])
        (item_mode, item) = old_directory.child(old[-1])
        old_directory.remove(old[-1])
        new_directory.add(new[-1], item_mode, item)


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

    def id(self):
        # Update object data
        self.object.set_raw_string(self._data.getvalue())
        return super(File, self).id()

    def store(self):
        # Update object data
        self.object.set_raw_string(self._data.getvalue())
        # Store
        oid = super(File, self).store()
        # Generate a tag with the sha1 that points to the sha1
        # That way, our blob object is not unreachable and cannot be garbage
        # collected
        self.storage.refs['refs/tags/%s' % oid ] = oid
        return oid


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

    def __init__(self, storage, commit):
        """Create a new Record. If commit is None, we create a Record based
        on a new empty Commit, i.e. a new commit with a new empty Tree."""
        if commit is None:
            self.root = Directory(storage, Tree())
        else:
            self.root = Directory(storage, storage[commit.tree])
        super(Record, self).__init__(storage, commit)

    def id(self):
        # This is not implemented, mainly because even changing the
        # object.*_time change the id, so it's rather useless or something
        # we can't really use to identify the commit. In general, you want
        # to identify the root, this is available via myrecord.root.id
        # anyhow.
        raise NotImplementedError


    def store(self):
        """Store a record."""
        self.object.parents = self.parents
        # XXX Add hostname based mail address?
        self.object.author = pwd.getpwuid(os.getuid()).pw_gecos.split(",")[0]
        self.object.committer = "Nodlehs"
        self.object.message = "Nodlehs auto-commit"
        self.object.author_timezone = \
            self.object.commit_timezone = \
            - time.timezone
        # XXX maybe checking for root tree items mtime would be better and
        # more accurate?
        self.object.author_time = \
            self.object.commit_time = \
            int(time.time())
        # Store root tree and the ref to the root tree
        self.object.tree = self.root.store()
        # Store us
        return super(Record, self).store()


class Storage(Repo):
    """Storage based on a repository."""

    def __init__(self, root):
        self.current_record_override = None
        # The next record
        self._next_record = None
        # XXX This should be an ordered set
        self.remotes = []
        super(Storage, self).__init__(root)
        # XXX Timer should be configurable.
        self._commiter = RepeatTimer(10.0, self.commit_and_push)
        self._commiter.daemon = True
        self._commiter.start()

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
                head = self[super(Storage, self).head()]
                self._next_record = Record(self, head)
                self._next_record.object.parents = [ head.id ]
            except KeyError:
                # Create a record based on brand new commit!
                self._next_record = Record(self, Commit())

        return self._next_record

    def commit(self):
        """Commit modification to the storage, if needed."""
        if self._next_record is not None:
            self.refs['refs/heads/master'] = self._next_record.store()
            self._next_record = None

    def push(self):
        """Push master and all tags to remotes."""
        refs = dict([ ("refs/tags/%s" % tag, ref)
                      for tag, ref in self.refs.as_dict("refs/tags").iteritems() ])
        refs["refs/heads/master"] = self.refs['refs/heads/master']
        # XXX Make that threaded?
        for remote in self.remotes:
            remote.push(refs)

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
