#!/usr/bin/env python

import unittest
import time
from nodlehs.utils import *

class TestUtils(unittest.TestCase):

    def test_Path_init(self):
        self.assert_(Path("/this/is/a/path/to//some/stuff/./and/..//..//it/goes/there"))
        self.assert_(Path(Path("/this/is/a/path/to//some/stuff/./and/..//..//it/goes/there")))
        self.assert_(Path([ "can" "i" "haz" "a" "path"]))
        self.assert_(Path("/").components == [])

    def test_Path_components(self):
        p = Path("/this/is/a/path/to//some/stuff/./and/..//..//it/goes/there")
        self.assert_(isinstance(p.components, list))
        self.assert_(len(p.components) == 9)
        self.assert_(len(p) == 9)
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

    def test_Path_pop(self):
        p = Path("a/b/c")
        self.assert_(p.pop() == "c")
        self.assert_(p.pop() == "b")
        self.assert_(p.pop() == "a")
        try:
            p.pop()
            self.assert_(False)
        except IndexError:
            self.assert_(True)

    def test_Path_item(self):
        p = Path("foo/bar")
        self.assert_(p[0] == "foo")
        self.assert_(p[1] == "bar")
        p[1] = "babar"
        self.assert_(p[0] == "foo")
        self.assert_(p[1] == "babar")
        self.assert_(p.path == "foo/babar")

    def test_Path_eq(self):
        self.assert_(Path("a/b") == Path("a/../a/b"))

    def test_Path_add(self):
        self.assert_(Path("a/b") + Path ("/c/d") == Path("a/b/c/d"))

    def _RepeatTimer_ran(self):
        self._rt_ran += 1

    def test_RepeatTimer(self):
        self._rt_ran = False
        timer = RepeatTimer(0.1, self._RepeatTimer_ran)
        timer.start()
        time.sleep(0.5)
        timer.cancel()
        self.assert_(self._rt_ran > 3)

    def test_OrderedSet(self):
        s = OrderedSet()
        s = OrderedSet([1, 3, 4, 5])
        self.assert_(1 in s)
        self.assert_(3 in s)
        s.add("hi")
        s.update(OrderedSet([1, 3, 8]))
        self.assert_(s[5] is 8)
        self.assert_(s == [ 1, 3, 4, 5, 'hi', 8 ])
        s.clear()
        self.assert_(s == [])

if __name__ == '__main__':
    unittest.main()
