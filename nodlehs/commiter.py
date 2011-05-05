#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# nodlehs.commiter -- Commiter objects
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


class TimeCommiter(RepeatTimer):
    """A commiter that commit every N seconds."""

    def __init__(self, box, time):
        super(TimeCommiter, self).__init__(time, box.Commit)
        self.daemon = True
