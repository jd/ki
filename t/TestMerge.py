#!/usr/bin/env python

import unittest
from nodlehs.merge import *

class TestMerge(unittest.TestCase):

    def test_merge_conflict(self):
        try:
            merge("hello world", "hello there", "hello you")
        except MergeConflictError as e:
            self.assert_(e.number_of_conflicts == 1)
        else:
            self.assert_(False)

    def test_merge_ok(self):
        self.assert_(merge("hello\nyou", "hello", "hello") == "hello\nyou")
        self.assert_(merge("hello\nyou", "hello", "hello\nyou") == "hello\nyou")

if __name__ == '__main__':
    unittest.main()
