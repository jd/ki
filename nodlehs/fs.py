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

from .storage import *
from .utils import Path


class Nodlehs(fuse.Operations):
    """The Nodlehs file system."""

    def __init__(self, root):
        self.start_time = time.time()
        self.storage = Storage(root)
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

    def readdir(self, path, offset):
        (mode, child) = self.storage.root.child(path)
        if isinstance(child, Directory):
            yield '.'
            yield '..'
            for entry in child:
                yield entry[1]

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

    def read(self, path, size, offset):
        try:
            (mode, child) = self.storage.root.child(path)
        except NotDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

        if not isinstance(child, File):
            raise fuse.FuseOSError(errno.EINVAL)

        return str(child)[offset:offset + size]

    def create(self, path, mode):
        return self.mknod(path, mode, None)

    def mknod(self, path, mode, dev):
        if not stat.S_ISREG(mode):
            raise fuse.FuseOSError(errno.EINVAL)
        if not self.storage.is_writable():
            raise fuse.FuseOSError(errno.EROFS)

        path = Path(path)

        try:
            (directory_mode, directory) = self.storage.next_record.root.child(path[:-1])
        except NotDirectory:
            raise fuse.FuseOSError(errno.ENOTDIR)
        except NoChild:
            raise fuse.FuseOSError(errno.ENOENT)

        # Add the file
        f = File(self, Blob())
        f.mtime = time.time()
        directory.add(path[-1], mode, f)
