#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# nodlehs.fs -- Fuse based file system
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

import fuse
fuse.fuse_python_api = (0, 2)
import errno
import os
import stat
import time
import posix
from decorator import decorator

from .storage import *
from .utils import Path

@decorator
def rw(func, self, *args, **kw):
    if not self.storage.is_writable():
        raise fuse.FuseOSError(errno.EROFS)

    return func(self, *args, **kw)


class Nodlehs(fuse.Operations):
    """The Nodlehs file system."""

    def __init__(self, root):
        self.start_time = time.time()
        self.storage = Storage(root)
        self.fds = {}
        super(Nodlehs, self).__init__()

    def access(self, path, amode):
        (mode, child) = self._get_child(path, File)

        if amode & posix.W_OK and not self.storage.is_writable():
            raise fuse.FuseOSError(errno.EACCESS)
        if amode & posix.X_OK:
            if not mode & S_IXUSR:
                raise fuse.FuseOSError(errno.EACCESS)

        return 0

    def getattr(self, path, fh=None):
        (mode, child) = self._resolve(path, fh)
        s = {}
        # Directories have no mode, so set one by default
        # XXX This mode should be a config option?
        if mode & stat.S_IFDIR:
            mode |= (stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP)
        elif mode & stat.S_IFLNK:
            mode |= (stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                     | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP
                     | stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
        s['st_mode'] = mode
        s['st_ino'] = 0
        s['st_dev'] = 0
        s['st_nlink'] = 1
        s['st_uid'] = os.getuid()
        s['st_gid'] = os.getgid()
        s['st_size'] = len(child)
        # Special case: for the root directory, there's no object at all, so
        # we return the start time as the ctime
        if path == '/':
            s['st_ctime'] = self.start_time
        else:
            # XXX accessing object is not that good.
            try:
                s['st_ctime'] = self.storage.current_record.object.commit_time
            # No record or no commit_time on the current record
            except (NoRecord, AttributeError):
                s['st_ctime'] = 0
        try:
            s['st_mtime'] = child.mtime
        except AttributeError:
            s['st_mtime']= s['st_ctime']
        # TODO: store atime internally?
        s['st_atime'] = 0
        return s

    def to_fd(self, mode, item):
        """Return a fd for item."""
        fd = len(self.fds)
        self.fds[fd] = (mode, item)
        return fd

    def opendir(self, path):
        return self.to_fd(*self._get_child(path, Directory))

    def readdir(self, path, fh=None):
        yield '.'
        yield '..'
        for entry in self._resolve(path, fh, Directory)[1]:
            yield entry[1]

    def release(self, path, fh):
        del self.fds[fh]

    def releasedir(self, path, fh):
        del self.fds[fh]

    def open(self, path, flags):
        (mode, child) = self._get_child(path, File)

        if not self.storage.is_writable() and flags & (os.O_WRONLY | os.O_RDWR):
            raise fuse.FuseOSError(errno.EROFS)

        return self.to_fd(mode, child)

    @rw
    def unlink(self, path):
        path = Path(path)
        (directory_mode, directory) = self._get_child(path[:-1], Directory)
        try:
            directory.remove(path[-1])
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

    rmdir = unlink

    @rw
    def _create(self, path, mode, obj):
        path = Path(path)
        (directory_mode, directory) = self._get_child(path[:-1], Directory)
        obj.mtime = time.time()
        directory.add(path[-1], mode, obj)

        return self.to_fd(mode, obj)

    def mkdir(self, path, mode):
        self._create(path, stat.S_IFDIR | mode, Directory(self.storage, Tree()))

    def create(self, path, mode):
        return self._create(path, mode, File(self.storage, Blob()))

    def mknod(self, path, mode, dev):
        if not stat.S_ISREG(mode):
            raise fuse.FuseOSError(errno.EINVAL)

        return self._create(path, mode, File(self.storage, Blob()))

    @rw
    def rename(self, old, new):
        old = Path(old)
        new = Path(new)

        try:
            (old_directory_mode, old_directory) = self.storage.next_record.root.child(old[:-1])
            (new_directory_mode, new_directory) = self.storage.next_record.root.child(new[:-1])
            (item_mode, item) = old_directory.child(old[-1])
            old_directory.remove(old[-1])
        except NotDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

        new_directory.add(new[-1], item_mode, item)


    @rw
    def chmod(self, path, mode):
        path = Path(path)

        try:
            (directory_mode, directory) = self.storage.next_record.root.child(path[:-1])
            (item_mode, item) = directory.child(path[-1])
            directory.remove(path[-1])
        except NotDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

        directory.add(path[-1], mode, item)

    @rw
    def link(self, target, source):
        target = Path(target)
        source = Path(source)

        try:
            (source_mode, source) = self.storage.next_record.root.child(source)
            (target_directory_mode, target_directory) = \
                self.storage.next_record.root.child(target[:-1])
        except NotDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

        target_directory.add(target[-1], source_mode, source)

    def _get_child(self, path, cls=None):
        """Get the mode and child of path.
        Also check that child is instance of cls."""
        try:
            (mode, child) = self.storage.root.child(Path(path))
        except NotDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)
        if cls is not None and not isinstance(child, cls):
            raise fuse.FuseOSError(errno.EINVAL)
        return (mode, child)

    def _resolve(self, path, fh=None, cls=None):
        """Resolve a file based on fh or path."""
        if fh is None:
            return self._get_child(path, cls)
        return self.fds[fh]

    def read(self, path, size, offset, fh=None):
        (mode, child) = self._resolve(path, fh, File)
        child.seek(offset)
        return child.read(size)

    @rw
    def write(self, path, data, offset, fh=None):
        (mode, child) = self._resolve(path, fh, File)
        child.seek(offset)
        return child.write(data)

    @rw
    def truncate(self, path, length, fh=None):
        return self._resolve(path, fh, File)[1].truncate(length)

    @rw
    def symlink(self, target, source):
        target = Path(target)
        (target_directory_mode, target_directory) = self._get_child(target[:-1])
        target_directory.add(target[-1], stat.S_IFLNK, Symlink(self.storage, Blob(), source))

    def readlink(self, path):
        return str(self._get_child(path, Symlink)[1])

    @rw
    def utimens(self, path, times=None):
        """Times is a (atime, mtime) tuple. If None use current time."""
        (mode, child) = self._get_child(path, File)
        if times is None:
            now = time.time()
            child.mtime = child.atime = now
        else:
            child.atime = times[0]
            child.mtime = times[1]
