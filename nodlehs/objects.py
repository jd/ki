#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# nodlehs.objects -- Stored objects
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
from dulwich.objects import Blob, Commit, Tree, S_IFGITLINK
import dulwich.diff_tree as diff_tree
import stat
import time
import socket
import os
import pwd
import cPickle
from .merge import *
from StringIO import StringIO

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

    def __init__(self, storage, obj=None):
        self.storage = storage
        self._object = obj

    @property
    def object(self):
        self._update(Storable.object)
        return self._object

    def _update(self, update_type):
        """Update the internal object."""
        raise NotImplementedError

    def store(self):
        """Store object into its storage.
        Return the object SHA1."""
        self._update(Storable.store)
        self.storage.object_store.add_object(self._object)
        return self._object.id

    def id(self):
        """Return the object SHA1 id.
        Note that this id can changed anytime, since data change all the time."""
        self._update(Storable.id)
        return self._object.id

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


class UnknownChangeType(Exception):
    pass


class Directory(Storable):
    """A directory."""

    def __init__(self, storage, obj=None):
        if obj is None:
            obj = Tree()
        # This is locally modified/added files which will belong to our tree
        # when we will dump ourselves.
        self.local_tree = {}
        super(Directory, self).__init__(storage, obj)

    def _update(self, update_type):
        for name, (mode, child) in self.local_tree.iteritems():
            # We store file with the GITLINK property. This is an hack to be
            # sure git will send us the whole blob when we fetch the tree.
            if isinstance(child, File):
                mode |= S_IFGITLINK
            if update_type == Storable.store:
                i = child.store()
            else:
                i = child.id()
            self._object.add(name, int(mode), i)

    def __iter__(self):
        return self.object.__iter__()

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
        path = Path(path)

        # If len(path) is 0, we are asked for ourselves.
        # This happens if the absolute path asked is /
        # Yeah, that means we are the r00t directory, indeed.
        if len(path) == 0:
            return (stat.S_IFDIR, self)

        (mode, child) = self._child_from_name(path[0])

        if len(path) == 1:
            return (mode, child)

        if isinstance(child, Directory):
            return child.child(path[1:])

        raise NotDirectory(child)

    def __delitem__(self, path):
        return self.remove(path)

    def __getitem__(self, path):
        return self.child(path)

    def __setitem__(self, path, value):
        # Value should be a tuple (mode, object)
        return self.add(path, value[0], value[1])

    def mkdir(self, path, directory=None):
        """Create a directory name with dir_object being the Directory object.
        If the directory at path already exists, do nothing.
        Returns the directory."""

        path = Path(path)
        subdir = self
        while path:
            curdir = path.pop(0)
            try:
                (mode, subdir) = subdir.child(curdir)
            except NoChild:
                subdir[curdir] = (stat.S_IFDIR, Directory(self.storage))
                subdir = subdir[curdir][1]

        return subdir

    def add(self, path, mode, f):
        """Add a file with name and mode attributes to directory."""

        path = Path(path)
        subdir = self.mkdir(path[:-1])
        subdir.local_tree[path[-1]] = (mode, f)
        subdir.mtime = time.time()

    def remove(self, path):
        """Remove a file with name from directory."""
        path = Path(path)
        subdir = self[path[:-1]][1]
        name = path[-1]
        try:
            # Try to delete in the local tree
            del subdir.local_tree[name]
        except KeyError:
            # The file was not in local_tree, try in self.object and raises
            # if it raises.
            try:
                del subdir.object[name]
            except KeyError:
                raise NoChild(name)
        else:
            # We succeeded to delete in local_tree, just try to delete in
            # self.object to be sure we deleted definitively.
            try:
                del subdir.object[name]
            except KeyError:
                pass

        subdir.mtime = time.time()

    def rename(self, old, new):
        old = Path(old)
        new = Path(new)

        (old_directory_mode, old_directory) = self.child(old[:-1])
        (new_directory_mode, new_directory) = self.child(new[:-1])
        (item_mode, item) = old_directory.child(old[-1])
        old_directory.remove(old[-1])
        new_directory.add(new[-1], item_mode, item)

    def merge_tree_changes(self, changes):
        """Merge a tree into this directory."""
        for change in changes:
            print change
            if change.type == diff_tree.CHANGE_DELETE:
                try:
                    mode, child = self[change.old.path]
                except NoChild, NotDirectory:
                    # Already deleted, good.
                    pass
                else:
                    # Only handle delete request if the delete operation would
                    # be done on the same file version.
                    if change.old.sha == child.id:
                        try:
                            del self[change.old.path]
                        except NoChild, NotDirectory:
                            # Maybe we did delete it on our side too, so, nothing bad, ignore!
                            pass
            elif change.type == diff_tree.CHANGE_MODIFY:
                try:
                    mode, child = self[change.old.path]
                except NoChild, NotDirectory:
                    self[change.new.path] = (change.new.mode, make_object(self.storage, change.new.sha))
                else:
                    # Only handle change operation if nothing has been
                    # changed in the mean time in our tree.
                    if change.old.sha == child.id:
                        self[change.new.path] = (change.new.mode, make_object(self.storage, change.new.sha))
                    else:
                        # Both have changed, try to merge
                        try:
                            child.merge(self.storage[change.old.sha].data,
                                        self.storage[change.new.sha].data)
                        except MergeConflictError as conflict:
                            # Store both files
                            self["%s.%s" % (change.old.path, change.old.sha)] = (change.old.mode,
                                                                                 make_object(self.storage,
                                                                                             change.old.sha))
                            self["%s.%s" % (change.new.path, change.new.sha)] = (change.new.mode,
                                                                                 make_object(self.storage,
                                                                                             change.new.sha))
                            # Store merged content in child
                            child.truncate(0)
                            child.write(conflict.content)
                        except MergeBinaryError, OSError:
                            # Store both files
                            self["%s.%s" % (change.old.path, change.old.sha)] = (change.old.mode,
                                                                                 make_object(self.storage,
                                                                                             change.old.sha))
                            self["%s.%s" % (change.new.path, change.new.sha)] = (change.new.mode,
                                                                                 make_object(self.storage,
                                                                                             change.new.sha))
            elif change.type == diff_tree.CHANGE_UNCHANGED:
                pass
            elif change.type == diff_tree.CHANGE_ADD \
                    or change.type == diff_tree.CHANGE_COPY:
                try:
                    mode, child = self[change.new.path]
                except NoChild:
                    # We do not have the move target, so let's add the file.
                    self[change.new.path] = (change.new.mode, make_object(self.storage, change.new.sha))
                except NotDirectory:
                    # XXX We cannot add this file here, so we need to add it somewhere else. Figure it out.
                    raise NotImplementedError
                else:
                    if change.new.sha != child.id:
                        # Conflict! The file already exists but is different.
                        # Yeah, this is ugly, but for now…
                        self["%s.%d" % (change.new.path, change.new.sha)] = (change.new.mode,
                                                                             make_object(self.storage, change.new.sha))
            elif change.type == diff_tree.CHANGE_RENAME:
                try:
                    mode, child = self[change.new.path]
                except NoChild, NotDirectory:
                    # We do not have the move target, so let's add the file.
                    try:
                        self[change.new.path] = (change.new.mode, make_object(self.storage, change.new.sha))
                    except NotDirectory:
                        # XXX Cannot add the file here, figure out what to do
                        raise NotImplementedError
                else:
                    if change.new.sha != child.id:
                        # Conflict! The file already exists but is different.
                        # Yeah, this is ugly, but for now…
                        self["%s.%d" % (change.new.path, change.new.sha)] = (change.new.mode,
                                                                             make_object(self.storage, change.new.sha))
                try:
                    mode, child = self[change.old.path]
                except NoChild, NotDirectory:
                    # We do not have the old file that was renamed, nothing to do.
                    pass
                else:
                    if change.old.sha == child.id:
                        # This is the same file that got renamed, so we can at least remove it.
                        del self[change.old.path]
            else:
                raise UnknownChangeType(change)


class File(Storable):
    """A file."""

    def __init__(self, storage, obj=None):
        if obj is None:
            obj = Blob()
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

    def _update(self, update_type):
        self._object.set_raw_string(self._data.getvalue())

    def store(self):
        # Store
        oid = super(File, self).store()
        # Generate a tag with the sha1 that points to the sha1
        # That way, our blob object is not unreachable and cannot be garbage
        # collected
        self.storage.refs['refs/tags/%s' % oid ] = oid
        return oid

    def merge(self, base, other):
        """Do a 3-way merge of other using base."""
        content = merge(self._data.getvalue(), base, other)
        self.truncate(0)
        self.write(content)


class Config(File):
    """A configuration.
    This is basically a dict stored in a File using pickling."""

    ref = 'refs/tags/config'

    def __init__(self, storage, default_values=dict()):
        """Create a new Config object."""
        super(Config, self).__init__(storage)
        self.config = default_values.copy()

    @classmethod
    def from_sha1(cls, storage, sha1):
        """Build a Config object based on a git SHA1 object."""
        return cls(storage, cPickle.loads(storage[sha1].data))

    def __getitem__(self, key):
        return self.config[key]

    def __setitem__(self, key, value):
        self.config[key] = value
        self.store()

    def _update(self, operation_type):
        self._object.set_raw_string(cPickle.dumps(self.config))

    def store(self):
        """Store the config in the storage."""
        # Bypass File.store.
        # Not sure my programming teacher would like it.
        oid = Storable.store(self)
        self.storage.refs[self.ref] = oid
        return oid


class Symlink(File):
    """A symlink."""

    def __init__(self, storage, obj=None, target="/"):
        if obj is None:
            obj = Blob()
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

    def __init__(self, storage, commit=None):
        """Create a new Record. If commit is None, we create a Record based
        on a new empty Commit, i.e. a new commit with a new empty Tree."""
        if commit is None:
            commit = Commit()
            need_update = True
            self.root = Directory(storage)
        else:
            self.root = Directory(storage, storage[commit.tree])
            need_update = False
        super(Record, self).__init__(storage, commit)
        self.parents = OrderedSet(self._object.parents)
        if need_update:
            self._update(Record.__init__)

    def _update(self, update_type):
        """Update commit information."""
        passwd = pwd.getpwuid(os.getuid())
        self._object.author = "%s <%s@%s>" % (passwd.pw_gecos.split(",")[0],
                                             passwd.pw_name,
                                             socket.getfqdn())
        self._object.committer = "Nodlehs"
        self._object.message = "Nodlehs auto-commit"
        self._object.author_timezone = \
            self._object.commit_timezone = \
            - time.timezone
        # XXX maybe checking for root tree items mtime would be better and
        # more accurate?
        self._object.author_time = \
            self._object.commit_time = \
            int(time.time())
        self._object.parents = self.parents
        if update_type == Storable.store:
            self._object.tree = self.root.store()
        else:
            self._object.tree = self.root.id()

    def commit_intervals(self, other, rev=True):
        """Return the list of commits between two records.
        Return None if not found.
        If rev is True, try the other way around too, in case objects are inversed."""
        commits = self.commit_history_list()
        ret = OrderedSet()

        for commit_set in commits:
            if other in commit_set:
                return ret
            else:
                ret.append(commit_set)

        if rev:
            return other.commit_intervals(self, False)
        else:
            return None

    def commit_history_list(self):
        """Return a list of commit history list for commit using
        breadth-first-search."""

        commits = OrderedSet([ set(self.parents) ])

        for commit_set in commits:
            for commit in commit_set:
                commits.add(set(commit.parents))

        return commits

    def find_common_ancestors(self, other):
        """Find the first common ancestors with another Record.

        This returns a set of common ancestors. This set will only have one
        item in it if the commits have one parent in common, but it can also
        returns a set with multiple commits in case commit1 and commit2 both
        have a merge commits as common ancestors (criss-cross merge).

        self-----------parentA\---
                               \- \------
                                 \       \---commitX---+
                                  \-                   |
                                    \-                 |
                                      \-               |
                                        \              |
                                          \-           |
                                            \commitY   |
                                                |      |
        other---------parentB-------------------+      |
                            \--------------------------+

        In such a case it would return set([ commitX, commitY ]).

        Also, it does not care about the order of the commits id in the
        parent field of a commit, and considers them as a set rather than a
        list.
        """
        commits1 = self.commit_history_list()
        commits2 = OrderedSet([ set(other.parents) ])

        for commit2_set in commits2:
            for commit1_set in commits1:
                if commit2_set.issubset(commit1_set):
                    return commit2_set
            for commit in commit2_set:
                commits2.add(set(commit.parents))

    def merge_commit(self, other):
        """Merge another commit into ourselves."""
        # XXX Maybe add some other barrier to be sure we are not merging one
        # of our parents, which would be utterly stupid.
        if other not in self.parents:
            # Update self before sending it to the LCA function
            common_ancestors = self.find_common_ancestors(self.update(),
                                                          self.storage[other])
            if len(common_ancestors) == 1:
                common_ancestor = common_ancestors.pop()
            else:
                # Criss-cross merge :( We merge the ancestors, and use that as a
                # base. This should be equivalent to what Git does in its
                # recursive merge method, if this code is correct, which shall
                # be proved.
                common_ancestor_r = Record(self.storage, self.storage[common_ancestors.pop()])
                for ancestor in common_ancestors:
                    common_ancestor_r.merge_commit(ancestor)
                common_ancestor = common_ancestor_r.store()

            print "    Looking for changes between:"
            print common_ancestor
            print other
            changes = diff_tree.RenameDetector(self.storage.object_store,
                                               self.storage[common_ancestor].tree,
                                               self.storage[other].tree).changes_with_renames()
            print "    Changes:"
            print changes
            self.root.merge_tree_changes(changes)
            self.parents.append(other)

