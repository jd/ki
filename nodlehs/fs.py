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
import errno
import os
import stat
import time
import posix
from decorator import decorator
from dulwich.objects import S_ISGITLINK, S_IFGITLINK

from .storage import *
from .utils import Path

@decorator
def rw(func, self, *args, **kw):
    if not self.storage.is_writable():
        raise fuse.FuseOSError(errno.EROFS)

    return func(self, *args, **kw)


class NodlehsFuse(fuse.Operations):
    """The Nodlehs file system."""

    def __init__(self, storage):
        self.start_time = time.time()
        self.storage = storage
        self.fds = {}
        super(NodlehsFuse, self).__init__()

    def access(self, path, amode):
        (mode, child) = self._get_child(path)

        if amode & posix.W_OK and not self.storage.is_writable():
            raise fuse.FuseOSError(errno.EACCES)
        if amode & posix.X_OK:
            if not mode & stat.S_IXUSR and not mode & stat.S_IFDIR:
                raise fuse.FuseOSError(errno.EACCES)

    def getattr(self, path, fh=None):
        (mode, child) = self._resolve(path, fh)
        s = {}
        if S_ISGITLINK(mode):
            # Transform gitlinks to files
            mode &= ~S_IFGITLINK
            mode |= stat.S_IFREG
        elif stat.S_ISDIR(mode):
            # Directories have no mode, so set one by default
            # XXX This mode should be a config option?
            mode |= (stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP)
        elif stat.S_ISLNK(mode):
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
            s['st_ctime'] = self.storage.head().object.commit_time
        try:
            s['st_mtime'] = child.mtime
        except AttributeError:
            s['st_mtime']= s['st_ctime']
        # TODO: store atime internally?
        s['st_atime'] = 0
        return s

    def to_fd(self, mode, item):
        """Return a fd for item."""
        # Collect all fd number
        k = self.fds.keys()
        k.sort()
        try:
            fd = k[-1] + 1
        except IndexError:
            fd = 0
        self.fds[fd] = (mode, item)
        return fd

    def opendir(self, path):
        return self.to_fd(*self._get_child(path, Directory))

    def readdir(self, path, fh=None):
        yield '.'
        yield '..'
        for entry in self._resolve(path, fh, Directory)[1]:
            yield entry

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
        try:
            self.storage.root[path] = (mode, obj)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)
        except NoDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)

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
        try:
            self.storage.root[new] = self.storage.root[old]
            del self.storage.root[old]
        except NotDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

    @rw
    def chmod(self, path, mode):
        try:
            self.storage.root[path] = (mode, self.storage.root[path][1])
        except NotDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

    @rw
    def link(self, target, source):
        # We do not support link operations
        raise fuse.FuseOSError(errno.EPERM)

    def _get_child(self, path, cls=None):
        """Get the mode and child of path.
        Also check that child is instance of cls."""
        try:
            (mode, child) = self.storage.root[path]
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

    def fsync(self, path, datasync, fh=None):
        (mode, child) = self._resolve(path, fh)
        child.store()

    fsyncdir = fsync
