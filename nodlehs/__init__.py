#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# nodlehs -- Distributed file system
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

__author__ = "Julien Danjou <julien@danjou.info>"

from .storage import *
from .fs import *
from .fuse import FUSE

def start(root, mountpoint):
    storage = Storage(root)
    FUSE(Nodlehs(storage), mountpoint, debug=True)
    # XXX Do not do that! It's probably not thread safe! We should split out
    #     the storage._commiter into another class and make it commit on
    #     cancel().
    storage.commit()
