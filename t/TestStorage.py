#!/usr/bin/env python

import unittest
import tempfile
import os
import shutil
from nodlehs.storage import *
from dulwich.objects import *
from dulwich.repo import MemoryRepo

class TestStorage(unittest.TestCase):

    def setUp(self):
        self.repo_path = tempfile.mktemp()
        os.mkdir(self.repo_path)
        self.storage = Storage.init_bare(self.repo_path)

    def test_Storage_root(self):
        self.assert_(self.storage.root is not None)
        self.assert_(self.storage.root is self.storage.head().root)
        self.assert_(self.storage.root is self.storage._next_record.root)

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

    def tearDown(self):
        shutil.rmtree(self.repo_path)

if __name__ == '__main__':
    unittest.main()
