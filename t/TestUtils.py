#!/usr/bin/env python

import unittest
import time
import operator
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

    def test_SortedList(self):
        a = SortedList()
        a.insert(1)
        a.insert(3)
        a.insert(2)
        self.assert_(a == [1, 2, 3])
        self.assert_(a.pop(1) == 2)
        self.assert_(a == [1, 3])
        a += [ 2 ]
        self.assert_(a == [1, 2, 3])
        self.assert_(a.index(3) == 2)
        self.assert_(a.index(1) == 0)
        self.assertRaises(ValueError, a.index, 0)

        a = SortedList([10, 20, 30])
        self.assert_(a.index_nearest_left(25) == 2)
        self.assert_(a.index_nearest_right(25) == 2)
        self.assert_(a.index_le(25) == 1)
        self.assert_(a.index_ge(25) == 2)
        self.assert_(a.index_le(20) == 1)
        self.assert_(a.index_ge(20) == 1)
        self.assert_(a.index_lt(20) == 0)
        self.assert_(a.index_gt(20) == 2)
        self.assert_(a.index_gt(21) == 2)

    def test_lrope_list(self):
        x = lrope.create_unknown_size([ "abc", "defg", "hijklm" ])
        self.assert_(str(x) == "abcdefghijklm")
        self.assert_(x[1] == "b")
        self.assert_(x[7] == "h")
        self.assert_(x[9] == "j")
        self.assert_(x[12] == "m")
        self.assert_(x[15] == "")
        self.assert_(x[1:3] == "bc")
        self.assert_(x[-1] == "m")
        self.assert_(x[-10:-1] == "defghijkl")
        x[1] = "z"
        self.assert_(x[1] == "z")
        self.assert_(str(x) == "azcdefghijklm")
        x[2] = "123"
        self.assert_(x[4] == "3")
        self.assert_(str(x) == "az123fghijklm")
        x[0] = "helloworldihasoverwrittenyou"
        self.assert_(str(x) == "helloworldihasoverwrittenyou")
        x[3] = "123345"
        self.assert_(str(x) == "hel123345dihasoverwrittenyou")
        self.assert_(len(x) == len("hel123345dihasoverwrittenyou"))

        x = lrope([])
        x[0] = "bonjour"


    def test_lrope_file(self):
        x = lrope.create_unknown_size([ "abc", "defg", "hijklm" ])
        x.seek(3)
        self.assert_(x.tell() == 3)
        x.seek(3, 1)
        self.assert_(x.tell() == 6)
        self.assert_(x.read(4) == "ghij")
        x.write("FOO")
        self.assert_(str(x) == "abcdefghijFOO")
        x.seek(1)
        self.assert_(str(x.read()) == "bcdefghijFOO")


if __name__ == '__main__':
    unittest.main()
