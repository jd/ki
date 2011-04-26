#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# nodlehs.merge -- Merge files
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

import subprocess
import tempfile
import os


class MergeError(Exception):
    """Error raised when a merge error occurs."""
    pass


class MergeConflictError(MergeError):
    """Error raised when a conflicts occurs during a merge."""

    def __init__(self, number_of_conflicts, content):
        self.number_of_conflicts = number_of_conflicts
        self.content = content
        if number_of_conflicts > 1:
            s = "s"
        else:
            s = ""
        super(MergeConflictError, self).__init__("%d conflict%s" % (number_of_conflicts, s))


class MergeBinaryError(MergeError):
    """Error raised when a merge is tried on binary content."""
    pass


def merge(current, base, other):
    """Use git-merge-file to merge content."""
    file_current = tempfile.mktemp()
    file_base = tempfile.mktemp()
    file_other = tempfile.mktemp()
    try:
        with file(file_current, "wb") as f:
            f.write(current)
        with file(file_base, "wb") as f:
            f.write(base)
        with file(file_other, "wb") as f:
            f.write(other)
        git_merge_file = subprocess.Popen([ "git", "merge-file", "--stdout",
                                            file_current, file_base, file_other ],
                                          stderr=subprocess.PIPE,
                                          stdout=subprocess.PIPE)
        git_merge_file.wait()
    finally:
        try:
            os.unlink(file_current)
        except:
            pass
        try:
            os.unlink(file_base)
        except:
            pass
        try:
            os.unlink(file_other)
        except:
            pass

    if git_merge_file.returncode == 255:
        raise MergeBinaryError
    elif git_merge_file.returncode > 0:
        raise MergeConflictError(git_merge_file.returncode,
                                 git_merge_file.communicate()[0])

    return git_merge_file.communicate()[0]



