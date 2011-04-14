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
import dulwich.index

from .storage import *
from .utils import Path


class Nodlehs(fuse.Fuse):
    """The Nodlehs file system."""

    def __init__(self, root):
        self.storage = Storage(root)
        super(Nodlehs, self).__init__()

    def getattr(self, path):
        try:
            (mode, obj) = self.storage.root.child(path)
        except NotDirectory:
            return -errno.ENOTDIR
        except NoChild:
            return -errno.ENOENT
        s = fuse.Stat()
        # Directories have no mode, so set one by default
        # XXX This mode should be a config option?
        if mode & stat.S_IFDIR:
            mode |= (stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP)
        s.st_mode = mode
        s.st_ino = 0
        s.st_dev = 0
        s.st_nlink = 1
        s.st_uid = os.getuid()
        s.st_gid = os.getgid()
        s.st_size = len(obj)
        # XXX accessing object is not that good.
        try:
            s.st_mtime = s.st_ctime = self.storage.current_record.object.commit_time
        # No record or no commit_time
        except (NoRecord, AttributeError):
            s.st_mtime = s.st_ctime = 0
        # TODO: store atime internally?
        s.st_atime = 0
        return s

    def readdir(self, path, offset):
        (mode, child) = self.storage.root.child(path)
        print child
        if isinstance(child, Directory):
            yield fuse.Direntry('.')
            yield fuse.Direntry('..')
            for entry in child:
                yield fuse.Direntry(entry[1])

    def open(self, path, flags):
        try:
            (mode, child) = self.storage.root.child(path)
        except NotDirectory:
            return -errno.ENOTDIR
        except NoChild:
            return -errno.ENOENT

        # Read access check
        if flags & (os.O_RDONLY | os.O_RDWR) and not mode & stat.S_IREAD:
            return -errno.EACCESS
        # Write access check
        if not self.storage.is_writable() \
                or flags & (os.O_RDONLY | os.O_WRONLY) \
                and not mode & stat.S_IWRITE:
            return -errno.EACCESS

    def read(self, path, size, offset):
        try:
            (mode, child) = self.storage.root.child(path)
        except NotDirectory:
            return -errno.ENOTDIR
        except NoChild:
            return -errno.ENOENT

        if not isinstance(child, File):
            return -errno.EINVAL

        return str(child)[offset:offset + size]

    def mknod(self, path, mode, dev):
        if not stat.S_ISREG(mode):
            return -errno.EINVAL
        if not self.storage.is_writable():
            return -errno.EROFS

        path = Path(path)

        try:
            (directory_mode, directory) = self.next_record.root.child(path[:-1])
        except NotDirectory:
            return -errno.ENOTDIR
        except NoChild:
            return -errno.ENOENT

        # Add the file
        directory.add(path[-1], mode, File(self, Blob()))
