#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ki.utils -- Ki utility library
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
        # XXX remove keys, use comparable objects!
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
        super(SortedList, self).__setitem__(i, value)
        self._update_keys()

    def __delitem__(self, i):
        super(SortedList, self).__delitem__(i)
        self._update_keys()

    def __iadd__(self, value):
        self.extend(value)
        self._update_keys()
        return self

    def keys(self):
        return self._keys

    def index(self, key):
        idx = bisect.bisect_left(self._keys, key)
        if self._keys[idx] == key:
            return idx
        raise ValueError

    def append(self, value):
        raise NotImplementedError

    def insert(self, object):
        if self._key:
            key = self._key(object)
        else:
            key = object
        where = self.index_nearest_left(key)
        super(SortedList, self).insert(where, object)
        self._update_keys()
        return where

    def insert_at(self, where, value):
        # XXX check if value is between -1 and +1 items
        super(SortedList, self).insert(where, value)
        self._update_keys()
        return where

    def index_nearest_left(self, key):
        return bisect.bisect_left(self._keys, key)

    def index_nearest_right(self, key):
        return bisect.bisect_right(self._keys, key)

    def index_le(self, key):
        """Return the index of the first element lesser or equal to key."""
        idx = self.index_nearest_left(key)
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
    such an operation."""

    def __init__(self, objects=[]):
        """Create a new lrope based on a list of objects.
        Format of the list must be:
        [ (size, object), (size, object), … ]"""
        offset = 0
        objects_offset = []
        for size, object in objects:
            objects_offset.append((offset, object))
            offset += size

        self._length = offset

        self._blocks = SortedList(objects_offset, key=self._key_func)

    @classmethod
    def create_unknown_size(cls, objects):
        """Create a rope based on a list of objects where length has not been precomputed."""
        return cls([ (len(o), o) for o in objects ] )

    def __iter__(self):
        return iter(self)

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
        if not isinstance(key, slice):
            key = slice(key, key + 1)
        self[key] = ""

    def __setitem__(self, key, value):
        # Remember that:
        # x[N] is insertion
        # x[N:M] is overwriting from N to M
        # x[N:] is overwriting from N to the end
        # etc
        if not isinstance(key, slice):
            key = slice(key, key)

        start, stop, step = key.indices(len(self))

        if step != 1:
            raise ValueError("steps other than 1 are not supported")

        # if start != stop: this is replacement
        # else: this is insertion
        if start != stop:
            first_block_index = self._blocks.index_le(start)
            last_block_index = self._blocks.index_le(stop - 1)

            if first_block_index == -1:
                # -1? This happens when the object has been created empty
                if start != 0:
                    raise IndexError("Trying to insert at index %d but this rope is empty" % start)
                self._blocks.insert((0, value))
            elif first_block_index == last_block_index:
                offset, block = self._blocks[first_block_index]
                self._blocks[first_block_index] = \
                    (offset, block[:start - offset] + value + block[stop - offset:])
            else:
                first_block_offset, first_block = self._blocks[first_block_index]
                last_block_offset, last_block = self._blocks[last_block_index]

                # Delete block between them
                if last_block_index - first_block_offset > 1:
                    del self._blocks[first_block_index + 1:last_block_index]

                if start == first_block_offset:
                    del self._blocks[first_block_index]
                    first_block_index -= 1
                else:
                    self._blocks[first_block_index] = (first_block_offset, first_block[:start - first_block_offset])

                if stop >= last_block_offset + len(last_block):
                    # Truncate the first block end
                    del self._blocks[first_block_index + 1]
                    # Insert the value with its offset
                    self._blocks.insert((start, value))
                else:
                    # Truncate the last block beginning
                    self._blocks[last_block_index] = (stop, last_block[stop - last_block_offset:])
                    # Insert the value with its offset
                    self._blocks.insert((start, value))
        elif start == 0 or start == len(self):
            # Insertion at the beginning/ending, easy case, we handle it.
            self._blocks.insert((0, value))
            for idx, (oldoffset, block) in enumerate(self._blocks[1:], 1):
                self._blocks[idx] = (oldoffset + len(value), block)
        else:
            raise ValueError("insertion at index != 0 is not supported")

        # Recompute length
        offset, block = self._blocks[len(self._blocks) - 1]
        self._length = offset + len(block)

    def __getitem__(self, key):
        if not isinstance(key, slice):
            if key < 0:
                key += len(self)
            key = slice(key, key + 1)

        start, stop, step = key.indices(len(self))

        if start == stop:
            return ""

        index = self._blocks.index_le(start)
        data = ""
        offset, block = self._blocks[index]
        block_start = start - offset

        while stop > offset:
            try:
                next_offset, next_block = self._blocks[index + 1]
            except IndexError:
                # Last element!
                next_offset = offset + len(block)
                next_block = ""

            block_end = min(next_offset, stop) - offset
            data += self._blocks[index][1][block_start:block_end:step]
            block_start = 0
            index += 1
            offset = next_offset
            block = next_block

        return data

    @property
    def blocks(self):
        return self._blocks

    def block_size_at(self, index):
        """Return size of block at index."""
        if index < len(self) - 1:
            return self._blocks[index + 1][0] - self._blocks[index][0]
        return self._blocks[index + 1][0] - len(self)

    def block_index_for_offset(self, offset):
        return self._blocks.index_le(offset)


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
