#!/usr/bin/env python

import unittest
import tempfile
import os
import shutil
import dbus.service
from nodlehs.storage import Storage, Branch
from nodlehs.objects import File, Directory, Record, NoChild
from dulwich.objects import *

from TestStorage import init_storage

class TestObjects(unittest.TestCase):

    def setUp(self):
        init_storage(self)

    def test_Record_find_common_ancestor(self):
        r1 = Record(self.storage)
        r2 = Record(self.storage)
        r1.root["a"] = (100644, File(self.storage))
        r2.parents.add(r1)
        self.assert_(len(r2.find_common_ancestors(r1)) is 0)
        self.assert_(len(r1.find_common_ancestors(r1)) is 0)
        r3 = Record(self.storage)
        r3.parents.add(r1)
        ret = r3.find_common_ancestors(r2)
        self.assert_(len(ret) is 1)
        self.assert_(ret.pop() is r1)
        ret = r2.find_common_ancestors(r3)
        self.assert_(len(ret) is 1)
        self.assert_(ret.pop() is r1)
        r4 = Record(self.storage)
        r4.parents.add(r2)
        r4.parents.add(r3)
        r5 = Record(self.storage)
        r5.parents.add(r2)
        r5.parents.add(r3)
        ret = r4.find_common_ancestors(r5)
        self.assert_(len(ret) is 2)
        self.assert_(ret == set([r2, r3]))

    def test_Record_intervals(self):
        r1 = Record(self.storage)
        r2 = Record(self.storage)
        r3 = Record(self.storage)
        r1.parents = [ r2 ]
        r2.parents = [ r3 ]
        self.assert_(r3.commit_intervals(r1) == [ set([ r2 ]) ])
        self.assert_(r2.commit_intervals(r1) == [])
        self.assert_(r1.commit_intervals(r3) == [ set([ r2 ] )])
        self.assert_(r3.commit_intervals(r1, False) is None)

    def test_Directory_mkdir(self):
        directory = Directory(self.storage, Tree())
        directory.mkdir("a/b/c")
        self.assert_(isinstance(directory["a"][1], Directory))
        self.assert_(isinstance(directory["a"][1]["b"][1], Directory))
        self.assert_(isinstance(directory["a"][1]["b"][1]["c"][1], Directory))

    def test_Directory_add(self):
        directory = Directory(self.storage, Tree())
        f = File(self.storage)
        directory["m/k/x"] = (0, f)
        self.assert_(isinstance(directory["m"][1], Directory))
        self.assert_(isinstance(directory["m"][1]["k"][1], Directory))
        self.assert_(isinstance(directory["m"][1]["k"][1]["x"][1], File))
        self.assert_(directory["m"][1]["k"][1]["x"][1] is f)

    def test_Directory_del(self):
        directory = Directory(self.storage, Tree())
        f = File(self.storage)
        directory["m/k/x"] = (0, f)
        del directory["m/k"]
        self.assert_(isinstance(directory["m"][1], Directory))
        self.assertRaises(NoChild, lambda: directory["m"][1]["k"])

    def tearDown(self):
        shutil.rmtree(self.repo_path)

if __name__ == '__main__':
    unittest.main()
