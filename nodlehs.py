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

from nodlehs.fuse import FUSE
from nodlehs.fs import Nodlehs

import sys

if len(sys.argv) != 3:
    print "usage: %s <repository> <mountpoint>" % sys.argv[0]
    sys.exit(1)

fuse = FUSE(Nodlehs(sys.argv[1]), sys.argv[2], debug=True)
