#!/usr/bin/env python

import unittest
from nodlehs.split import split

class TestSplit(unittest.TestCase):

    def test_split(self):
        with file(__file__) as f:
            result = list(split(f))
        with file(__file__) as f:
            self.assert_(f.read() == "".join(result))

if __name__ == '__main__':
    unittest.main()
