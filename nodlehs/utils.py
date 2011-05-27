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
import threading
import uuid
import bisect
import collections

class Path(object):
    """Magical path object.
    This allow to manipulate path very easily."""

    def __init__(self, path):
        """Create a new path object.
        You can build it using a string, a Path, or a list."""
        if isinstance(path, str):
            path = os.path.normpath(path)
            if path == '/':
                self.components = []
            elif path[0] == '/':
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

    def pop(self, value=None):
        if value is not None:
            return self.components.pop(value)
        return self.components.pop()

    def __iter__(self):
        """Return an interator on components."""
        return self.components.__iter__()

    def __getitem__(self, key):
        return self.components[key]

    def __setitem__(self, key, value):
        self.components[key] = value

    def __len__(self):
        return len(self.components)

    def __add__(self, value):
        if isinstance(value, Path):
            return Path(self.components + value.components)
        raise TypeError

    def __eq__(self, value):
        return self.components == value.components

    def __repr__(self):
        return "<" + self.__class__.__name__ + " " + hex(id(self)) + " for " + self.path + ">"


class RepeatTimer(threading._Timer):

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                self.function(*self.args, **self.kwargs)

class OrderedSet(list):

    def __init__(self, iterable=[]):
        for item in iterable:
            self.append(item)

    def __add__(self, other):
        return OrderedSet(super(OrderedSet, self).__add__(other))

    def append(self, item):
        if not item in self:
            return super(OrderedSet, self).append(item)

    def update(self, *args):
        for s in args:
            for e in s:
                 self.append(e)

    def add(self, item):
        return self.append(item)

    def discard(self, item):
        return self.remove(item)

    def clear(self):
        del self[:]

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, super(OrderedSet, self).__repr__())


class SortedList(list):

    """An ordered list where it's fast to find things using bsearch."""

    def __init__(self, iterable=[], key=None):
        self._key = key
        super(SortedList, self).__init__(sorted(iterable, key=key))
        self._update_keys()

    def _update_keys(self):
        if self._key:
            self._keys = [ self._key(item) for item in self ]
        else:
            self._keys = self

    def __setslice__(self, i, j):
        raise NotImplementedError

    def __setitem__(self, i, value):
        if i > 0 and value < self[i - 1]:
            raise ValueError("value is lesser than the previous item")
        try:
            if value > self[i + 1]:
                raise ValueError("value is greater than the next item")
        except IndexError:
            pass
        return super(SortedList, self).__setitem__(i, value)

    def __iadd__(self, value):
        self.extend(value)
        return self

    def keys(self):
        return self._keys

    def index(self, key):
        idx = bisect.bisect_left(self._keys, key)
        if self._keys[idx] == key:
            return idx
        raise ValueError

    def insert(self, object):
        if self._key:
            key = self._key(object)
        else:
            key = object
        super(SortedList, self).insert(bisect.bisect(self._keys, key), object)
        self._update_keys()

    def index_nearest_left(self, key):
        return bisect.bisect_left(self._keys, key)

    def index_nearest_right(self, key):
        return bisect.bisect_right(self._keys, key)

    def index_le(self, key):
        """Return the index of the first element lesser or equal to key."""
        idx = bisect.bisect_left(self._keys, key)
        if idx < len(self._keys) and self._keys[idx] == key:
            return idx
        return idx - 1

    def index_lt(self, key):
        return bisect.bisect_left(self._keys, key) - 1

    def index_ge(self, key):
        """Return the index of the first element greater or equal to key."""
        idx = bisect.bisect_right(self._keys, key)
        if idx > 0 and self._keys[idx - 1] == key:
            return idx - 1
        return idx

    def index_gt(self, key):
        return bisect.bisect_right(self._keys, key)

    def extend(self, iterable):
        for item in iterable:
            self.insert(item)


class lrope(collections.MutableSequence):

    """An implementation of the rope data structure using a list.
    Instead of using a binary tree, this implementation uses a sorted array,
    using binary search to find the interesting block, staying O(log n) for
    such an operation.
    It supports both list interface (getitem, setitem) and file interface
    (seek, read, write)."""

    def __init__(self, objects=[]):
        """Create a new lrope based on a list of objects.
        Format of the list must be:
        [ (size, object), (size, object), … ]"""
        offset = 0
        # If objects is empty
        if len(objects) == 0:
            objects_offset = [ (0, '') ]
        else:
            objects_offset = []
            for size, object in objects:
                objects_offset.append((offset, object))
                offset += size

        self._length = offset

        self._objects = SortedList(objects_offset, key=self._key_func)
        self._offset = 0 # Used by file interface

    @classmethod
    def create_unknown_size(cls, objects):
        """Create a rope based on a list of objects where length has not been precomputed."""
        return cls([ (len(o), o) for o in objects ] )

    def __iter__(self):
        return iter(self._objects)

    def seek(self, offset, whence=0):
        if whence == 0:
            self._offset = offset
        elif whence == 1:
            self._offset += offset
        elif whence == 2:
            self._offset = len(self) + offset

    def tell(self):
        return self._offset

    def truncate(size=None):
        if size == None:
            size = self._offset

        block_index = self._objects.index_le(size)
        block_offset, block = self._objects[block_index]
        if size == block_offset:
            del self._objects[block_index]
        else:
            self._objects[block_index] = (block_offset, block[:size - block_offset])

    def write(self, s):
        self[self._offset] = s

    def read(self, size=None):
        if size != None:
            size += self._offset
        read = self[self._offset:size]
        self._offset = min(size, len(self))
        return read

    def insert(self, index, object):
        self[index] = object

    def __str__(self):
        return self[:]

    @staticmethod
    def _key_func(item):
        return item[0]

    def __len__(self):
        return self._length

    def __delitem__(self, key):
        raise NotImplementedError

    def __setitem__(self, start, value):
        if not isinstance(start, int):
            raise ValueError("key must be int")

        stop = start + len(value)

        if start == stop:
            return

        first_block_index = self._objects.index_le(start)
        last_block_index = self._objects.index_le(stop - 1)

        # The len(self) only changes if we modify the last block
        if last_block_index == len(self._objects) - 1:
            size_may_have_changed = True
        else:
            size_may_have_changed = False

        if first_block_index == last_block_index:
            offset, block = self._objects[first_block_index]
            self._objects[first_block_index] = \
                (offset, block[:start - offset] + value + block[stop - offset:])
        else:
            first_block_offset, first_block = self._objects[first_block_index]
            last_block_offset, last_block = self._objects[last_block_index]

            # Delete block between them
            if last_block_index - first_block_offset > 1:
                del self._objects[first_block_index + 1:last_block_index]

            # Truncate the first block end
            if start == first_block_offset:
                del self._objects[first_block_index]
                first_block_index -= 1
            else:
                self._objects[first_block_index] = (first_block_offset, first_block[:start - first_block_offset])

            # Truncate the last block start
            if stop >= last_block_offset + len(last_block):
                del self._objects[first_block_index + 1]
            else:
                self._objects[last_block_index] = (stop, last_block[stop - last_block_offset:])

            # Insert the value with its offset
            self._objects.insert((start, value))

        # The len(self) only changes if we modified the last block
        if size_may_have_changed:
            offset, block = self._objects[len(self._objects) - 1]
            self._length = offset + len(block)

    def __getitem__(self, key):
        if not isinstance(key, slice):
            if key < 0:
                key += len(self)
            key = slice(key, key + 1)

        start, stop, step = key.indices(len(self))

        if start == stop:
            return ""

        # [ (offset, data), …, (offset, data), … ]
        #                      ^
        #                      `- index

        index = self._objects.index_le(start)
        offset, data = self._objects[index]

        try:
            next_offset, next_data = self._objects[index + 1]
        except IndexError:
            # Last element!
            next_offset = offset + len(data)
            next_data = ""

        data_start = start - offset
        data_length = next_offset - offset
        data_length_available = data_length - data_start
        length_wanted = stop - start
        length_to_return = min(data_length_available, length_wanted)
        data_stop = length_to_return + data_start

        return data[data_start:data_stop:step] \
            + self[start + length_to_return:stop:step]


class lmolrope(lrope):
    """A lrope class which tracks the lowest modified offset."""

    def __init__(self, objects=[]):
        self._lmo = None
        super(lmolrope, self).__init__(objects)

    def __setitem__(self, key, value):
        super(lmolrope, self).__setitem__(key, value)
        if self._lmo is None or key < self._lmo:
            self._lmo = key

    @property
    def lmo(self):
        """Lowest modified offset."""
        return self._lmo

    @property
    def lmb(self):
        """Lowest modified block."""
        return self._objects.index_le(self._lmo)


class SingletonType(type):
    """Singleton metaclass."""

    def __call__(cls, *args, **kwargs):
        try:
            return cls.__instance
        except:
            cls.__instance = type.__call__(cls, *args, **kwargs)
            return cls.__instance


class Singleton(object):
    """Singleton class."""

    __metaclass__ = SingletonType

def dbus_clean_name(s):
    """Return a string cleaned from any disallowed item in a D-Bus path."""
    return filter(lambda x: 'a' <= x <= 'z' or 'A' <= x <= 'Z' or '0' <= x <= '9' or x == '_', s)

def dbus_uuid():
    """Return an UUID usable in a D-Bus object path."""
    return "".join(map(lambda x: x == '-' and '_' or x, str(uuid.uuid4())))
