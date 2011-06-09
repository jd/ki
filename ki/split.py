#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ki.split -- Bup (rsync) based file split handling
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

import sys
sys.path.append("/usr/lib/bup")

from bup._helpers import splitbuf as _splitbuf
from bup.hashsplit import BLOB_MAX
try:
    from bup.hashsplit import BLOB_HWM as BLOB_READ_SIZE
except ImportError:
    from bup.hashsplit import BLOB_READ_SIZE as BLOB_READ_SIZE


def get_file_block(f):
    block = f.read(BLOB_READ_SIZE)
    while block:
        yield block
        block = f.read(BLOB_READ_SIZE)


def splitbuf(block):
    while True:
        ofs = min(BLOB_MAX, _splitbuf(block)[0])
        if ofs:
            yield buffer(block, 0, ofs)
            block = buffer(block, ofs)
        else:
            # Cannot find where to split, need more data!
            break


def split(f):
    """Split a file, yielding small block."""
    block = ""
    for read_block in get_file_block(f):
        block += read_block
        for split_block in splitbuf(block):
            yield split_block
            block = buffer(block, len(split_block))
    # If there's data remaining which impossible to split, cut it in small
    # pieces by hand.
    while len(block) >= BLOB_MAX:
        yield buffer(block, 0, BLOB_MAX)
        block = buffer(block, BLOB_MAX)
    if len(block):
        yield block
