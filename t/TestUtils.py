#!/usr/bin/env python

import unittest
from nodlehs.utils import *

class TestUtils(unittest.TestCase):

    def test_Path_init(self):
        Path("/this/is/a/path/to//some/stuff/./and/..//..//it/goes/there")
        Path(Path("/this/is/a/path/to//some/stuff/./and/..//..//it/goes/there"))
        Path([ "can" "i" "haz" "a" "path"])

    def test_Path_components(self):
        p = Path("/this/is/a/path/to//some/stuff/./and/..//..//it/goes/there")
        self.assert_(isinstance(p.components, list))
        self.assert_(len(p.components) == 9)
        self.assert_(p.components == ['this', 'is', 'a', 'path', 'to', 'some', 'it', 'goes', 'there'])

    def test_Path_iter(self):
        p = Path("foo/bar")
        i = 0
        for c in p:
            if i == 0:
                self.assert_(c == "foo")
            else:
                self.assert_(c == "bar")
            i += 1

    def test_Path_item(self):
        p = Path("foo/bar")
        self.assert_(p[0] == "foo")
        self.assert_(p[1] == "bar")
        p[1] = "babar"
        self.assert_(p[0] == "foo")
        self.assert_(p[1] == "babar")
        self.assert_(p.path == "foo/babar")


if __name__ == '__main__':
    unittest.main()
