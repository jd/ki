#!/usr/bin/env python

import unittest
import tempfile
import os
import shutil
from nodlehs.storage import *
from dulwich.objects import *

class TestStorage(unittest.TestCase):

    def setUp(self):
        self.repo_path = tempfile.mktemp()
        os.mkdir(self.repo_path)
        self.storage = Storage.init_bare(self.repo_path)

    def test_Storage_root(self):
        self.assert_(self.storage.root is not None)
        self.assert_(self.storage.root is self.storage.head().root)
        self.assert_(self.storage.root is self.storage._next_record.root)

    def test_Storage_find_common_ancestor(self):
        r1 = Record(self.storage)
        r2 = Record(self.storage)
        r1.root["a"] = (100644, File(self.storage, Blob()))
        r2.parents.add(r1)
        self.assert_(len(self.storage.find_common_ancestors(r2, r1)) is 0)
        self.assert_(len(self.storage.find_common_ancestors(r1, r2)) is 0)
        r3 = Record(self.storage)
        r3.parents.add(r1)
        ret = self.storage.find_common_ancestors(r3, r2)
        self.assert_(len(ret) is 1)
        self.assert_(ret.pop() is r1)
        ret = self.storage.find_common_ancestors(r2, r3)
        self.assert_(len(ret) is 1)
        self.assert_(ret.pop() is r1)
        r4 = Record(self.storage)
        r4.parents.add(r2)
        r4.parents.add(r3)
        r5 = Record(self.storage)
        r5.parents.add(r2)
        r5.parents.add(r3)
        ret = self.storage.find_common_ancestors(r4, r5)
        self.assert_(len(ret) is 2)
        self.assert_(ret == set([r2, r3]))

    def test_Directory_mkdir(self):
        directory = Directory(self.storage, Tree())
        directory.mkdir("a/b/c")
        self.assert_(isinstance(directory["a"][1], Directory))
        self.assert_(isinstance(directory["a"][1]["b"][1], Directory))
        self.assert_(isinstance(directory["a"][1]["b"][1]["c"][1], Directory))

    def test_Directory_add(self):
        directory = Directory(self.storage, Tree())
        f = File(self.storage, Blob())
        directory["m/k/x"] = (0, f)
        self.assert_(isinstance(directory["m"][1], Directory))
        self.assert_(isinstance(directory["m"][1]["k"][1], Directory))
        self.assert_(isinstance(directory["m"][1]["k"][1]["x"][1], File))
        self.assert_(directory["m"][1]["k"][1]["x"][1] is f)

    def test_Directory_del(self):
        directory = Directory(self.storage, Tree())
        f = File(self.storage, Blob())
        directory["m/k/x"] = (0, f)
        del directory["m/k"]
        self.assert_(isinstance(directory["m"][1], Directory))
        self.assertRaises(NoChild, lambda: directory["m"][1]["k"])

    def tearDown(self):
        shutil.rmtree(self.repo_path)

if __name__ == '__main__':
    unittest.main()
