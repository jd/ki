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

    def getattr(self, path, fh=None):
        try:
            (mode, obj) = self.storage.root.child(path)
        except NotDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)
        s = {}
        # Directories have no mode, so set one by default
        # XXX This mode should be a config option?
        if mode & stat.S_IFDIR:
            mode |= (stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP)
        s['st_mode'] = mode
        s['st_ino'] = 0
        s['st_dev'] = 0
        s['st_nlink'] = 1
        s['st_uid'] = os.getuid()
        s['st_gid'] = os.getgid()
        s['st_size'] = len(obj)
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
            s['st_mtime'] = obj.mtime
        except AttributeError:
            s['st_mtime']= s['st_ctime']
        # TODO: store atime internally?
        s['st_atime'] = 0
        return s

    def to_fd(self, item):
        """Return a fd for item."""
        fd = len(self.fds)
        self.fds[fd] = item
        return fd

    def opendir(self, path):
        try:
            (mode, child) = self.storage.root.child(path)
        except NotDirectory:
            raise FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

        if not isinstance(child, Directory):
            raise FuseOSError(errno.ENOTDIR)

        return self.to_fd(child)

    def readdir(self, path, fh):
        yield '.'
        yield '..'
        for entry in self.fds[fh]:
            yield entry[1]

    def release(self, path, fh):
        del self.fds[fh]

    def releasedir(self, path, fh):
        del self.fds[fh]

    def open(self, path, flags):
        try:
            (mode, child) = self.storage.root.child(path)
        except NotDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

        # Read access check
        if flags & (os.O_RDONLY | os.O_RDWR) and not mode & stat.S_IREAD:
            raise FuseOSError(errno.EACCESS)
        # Write access check
        if not self.storage.is_writable() \
                or flags & (os.O_RDONLY | os.O_WRONLY) \
                and not mode & stat.S_IWRITE:
            raise fuse.FuseOSError(errno.EACCESS)

        return self.to_fd(child)

    @rw
    def unlink(self, path):
        path = Path(path)

        try:
            (directory_mode, directory) = self.storage.next_record.root.child(path[:-1])
        except NotDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

        try:
            directory.remove(path[-1])
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

    rmdir = unlink

    @rw
    def _create(self, path, mode, obj):
        path = Path(path)

        try:
            (directory_mode, directory) = self.storage.next_record.root.child(path[:-1])
        except NotDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

        obj.mtime = time.time()
        directory.add(path[-1], mode, obj)

        return self.to_fd(obj)

    def mkdir(self, path, mode):
        return self._create(path, stat.S_IFDIR | mode, Directory(self.storage, Tree()))

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

    def _resolve(self, path, fh=None):
        """Resolve a file based on fh or path."""
        if fh is None:
            try:
                (mode, child) = self.storage.root.child(Path(path))
            except NotDirectory:
                raise fuse.FuseOSError(errno.ENOTDIR)
            except NoChild:
                raise fuse.FuseOSError(errno.ENOENT)
            if not isinstance(child, File):
                raise fuse.FuseOSError(errno.EINVAL)
            return child
        return self.fds[fh]

    def read(self, path, size, offset, fh):
        child = self._resolve(path, fh)
        child.seek(offset)
        return child.read(size)

    @rw
    def write(self, path, data, offset, fh=None):
        child = self._resolve(path, fh)
        child.seek(offset)
        return child.write(data)

    @rw
    def truncate(self, path, length, fh=None):
        print self.fds
        print fh
        print path
        child = self._resolve(path, fh)
        print repr(child)
        return child.truncate(length)

