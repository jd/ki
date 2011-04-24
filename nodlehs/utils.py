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

try:
    # Python >= 2.7
    from collections import OrderedDict
except ImportError:
    from collections import MutableMapping

    def _recursive_repr(user_function):
        'Decorator to make a repr function return "..." for a recursive call'
        repr_running = set()

        def wrapper(self):
            key = id(self), get_ident()
            if key in repr_running:
                return '...'
            repr_running.add(key)
            try:
                result = user_function(self)
            finally:
                repr_running.discard(key)
            return result

        # Can't use functools.wraps() here because of bootstrap issues
        wrapper.__module__ = getattr(user_function, '__module__')
        wrapper.__doc__ = getattr(user_function, '__doc__')
        wrapper.__name__ = getattr(user_function, '__name__')
        return wrapper

    class OrderedDict(dict):
        'Dictionary that remembers insertion order'
        # An inherited dict maps keys to values.
        # The inherited dict provides __getitem__, __len__, __contains__, and get.
        # The remaining methods are order-aware.
        # Big-O running times for all methods are the same as for regular dictionaries.

        # The internal self.__map dictionary maps keys to links in a doubly linked list.
        # The circular doubly linked list starts and ends with a sentinel element.
        # The sentinel element never gets deleted (this simplifies the algorithm).
        # Each link is stored as a list of length three:  [PREV, NEXT, KEY].

        def __init__(self, *args, **kwds):
            '''Initialize an ordered dictionary.  Signature is the same as for
            regular dictionaries, but keyword arguments are not recommended
            because their insertion order is arbitrary.

            '''
            if len(args) > 1:
                raise TypeError('expected at most 1 arguments, got %d' % len(args))
            try:
                self.__root
            except AttributeError:
                self.__root = root = [None, None, None]     # sentinel node
                PREV = 0
                NEXT = 1
                root[PREV] = root[NEXT] = root
                self.__map = {}
            self.__update(*args, **kwds)

        def __setitem__(self, key, value, PREV=0, NEXT=1, dict_setitem=dict.__setitem__):
            'od.__setitem__(i, y) <==> od[i]=y'
            # Setting a new item creates a new link which goes at the end of the linked
            # list, and the inherited dictionary is updated with the new key/value pair.
            if key not in self:
                root = self.__root
                last = root[PREV]
                last[NEXT] = root[PREV] = self.__map[key] = [last, root, key]
            dict_setitem(self, key, value)

        def __delitem__(self, key, PREV=0, NEXT=1, dict_delitem=dict.__delitem__):
            'od.__delitem__(y) <==> del od[y]'
            # Deleting an existing item uses self.__map to find the link which is
            # then removed by updating the links in the predecessor and successor nodes.
            dict_delitem(self, key)
            link = self.__map.pop(key)
            link_prev = link[PREV]
            link_next = link[NEXT]
            link_prev[NEXT] = link_next
            link_next[PREV] = link_prev

        def __iter__(self, NEXT=1, KEY=2):
            'od.__iter__() <==> iter(od)'
            # Traverse the linked list in order.
            root = self.__root
            curr = root[NEXT]
            while curr is not root:
                yield curr[KEY]
                curr = curr[NEXT]

        def __reversed__(self, PREV=0, KEY=2):
            'od.__reversed__() <==> reversed(od)'
            # Traverse the linked list in reverse order.
            root = self.__root
            curr = root[PREV]
            while curr is not root:
                yield curr[KEY]
                curr = curr[PREV]

        def __reduce__(self):
            'Return state information for pickling'
            items = [[k, self[k]] for k in self]
            tmp = self.__map, self.__root
            del self.__map, self.__root
            inst_dict = vars(self).copy()
            self.__map, self.__root = tmp
            if inst_dict:
                return (self.__class__, (items,), inst_dict)
            return self.__class__, (items,)

        def clear(self):
            'od.clear() -> None.  Remove all items from od.'
            try:
                for node in self.__map.itervalues():
                    del node[:]
                self.__root[:] = [self.__root, self.__root, None]
                self.__map.clear()
            except AttributeError:
                pass
            dict.clear(self)

        update = __update = MutableMapping.update
        keys = MutableMapping.keys
        values = MutableMapping.values
        items = MutableMapping.items
        iterkeys = MutableMapping.iterkeys
        itervalues = MutableMapping.itervalues
        iteritems = MutableMapping.iteritems
        __ne__ = MutableMapping.__ne__

        def viewkeys(self):
            "od.viewkeys() -> a set-like object providing a view on od's keys"
            return KeysView(self)

        def viewvalues(self):
            "od.viewvalues() -> an object providing a view on od's values"
            return ValuesView(self)

        def viewitems(self):
            "od.viewitems() -> a set-like object providing a view on od's items"
            return ItemsView(self)

        __marker = object()

        def pop(self, key, default=__marker):
            if key in self:
                result = self[key]
                del self[key]
                return result
            if default is self.__marker:
                raise KeyError(key)
            return default

        def setdefault(self, key, default=None):
            'od.setdefault(k[,d]) -> od.get(k,d), also set od[k]=d if k not in od'
            if key in self:
                return self[key]
            self[key] = default
            return default

        def popitem(self, last=True):
            '''od.popitem() -> (k, v), return and remove a (key, value) pair.
            Pairs are returned in LIFO order if last is true or FIFO order if false.

            '''
            if not self:
                raise KeyError('dictionary is empty')
            key = next(reversed(self) if last else iter(self))
            value = self.pop(key)
            return key, value

        @_recursive_repr
        def __repr__(self):
            'od.__repr__() <==> repr(od)'
            if not self:
                return '%s()' % (self.__class__.__name__,)
            return '%s(%r)' % (self.__class__.__name__, self.items())

        def copy(self):
            'od.copy() -> a shallow copy of od'
            return self.__class__(self)

        @classmethod
        def fromkeys(cls, iterable, value=None):
            '''OD.fromkeys(S[, v]) -> New ordered dictionary with keys from S
            and values equal to v (which defaults to None).

            '''
            d = cls()
            for key in iterable:
                d[key] = value
            return d

        def __eq__(self, other):
            '''od.__eq__(y) <==> od==y.  Comparison to another OD is order-sensitive
            while comparison to a regular mapping is order-insensitive.

            '''
            if isinstance(other, OrderedDict):
                return len(self)==len(other) and \
                       all(_imap(_eq, self.iteritems(), other.iteritems()))
            return dict.__eq__(self, other)

class OrderedSet(OrderedDict, collections.MutableSet):

    def __init__(self, iterable):
        super(OrderedSet, self).__init__()
        for i in iterable:
            self.add(i)

    def update(self, *args, **kwargs):
        if kwargs:
            raise TypeError("update() takes no keyword arguments")

        for s in args:
            for e in s:
                 self.add(e)

    def add(self, elem):
        self[elem] = None

    def discard(self, elem):
        self.pop(elem, None)

    def __le__(self, other):
        return all(e in other for e in self)

    def __lt__(self, other):
        return self <= other and self != other

    def __ge__(self, other):
        return all(e in self for e in other)

    def __gt__(self, other):
        return self >= other and self != other

    def __repr__(self):
        return 'OrderedSet([%s])' % (', '.join(map(repr, self.keys())))

    def __str__(self):
        return '{%s}' % (', '.join(map(repr, self.keys())))

    def __getitem__(self, key):
        return self.keys().__getitem__(key)

    difference = property(lambda self: self.__sub__)
    difference_update = property(lambda self: self.__isub__)
    intersection = property(lambda self: self.__and__)
    intersection_update = property(lambda self: self.__iand__)
    issubset = property(lambda self: self.__le__)
    issuperset = property(lambda self: self.__ge__)
    symmetric_difference = property(lambda self: self.__xor__)
    symmetric_difference_update = property(lambda self: self.__ixor__)
    union = property(lambda self: self.__or__)
