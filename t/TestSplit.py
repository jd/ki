#!/usr/bin/env python

import unittest
import random
from nodlehs.split import split


class RandomizedDataFile(object):

    def __init__(self):
        self._offset = 0
        self._size = 5000000
        self._data = "".join([ chr(random.getrandbits(8)) for i in range(self._size) ])

    def read(self, size=None):
        if size == None:
            end = self._size - self._offset
        else:
            end = min(self._size, self._offset + size)
        ret = self._data[self._offset:end]
        self._offset = end
        return ret

    def seek(self, where):
        self._offset = max(0, min(where, self._size))


class TestSplit(unittest.TestCase):

    def test_split(self):
        f = RandomizedDataFile()
        result = list(split(f))
        f.seek(0)
        self.assert_(f.read() == "".join(map(str, result)))

if __name__ == '__main__':
    unittest.main()
