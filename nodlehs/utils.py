#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# nodlehs.utils -- Nodlehs utility library
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

import os

class Path(object):
    """Magical path object.
    This allow to manipulate path very easily."""

    def __init__(self, path):
        """Create a new path object.
        You can build it using a string, a Path, or a list."""
        if isinstance(path, str):
            path = os.path.normpath(path)
            if path[0] == '/':
                self.path = path[1:]
            else:
                self.path = path
        elif isinstance(path, list):
            self.components = path
        else:
            # Assume it's a Path object
            self.components = path.components

    @property
    def path(self):
        return os.sep.join(self.components)

    @path.setter
    def path(self, value):
        self.components = value.split(os.sep)

    def __iter__(self):
        """Return an interator on components."""
        return self.components.__iter__()

    def __getitem__(self, key):
        return self.components[key]

    def __setitem__(self, key, value):
        self.components[key] = value

    def __repr__(self):
        return "<" + self.__class__.__name__ + " " + hex(id(self)) + " for " + self.path + ">"
