#!/usr/bin/env python

import unittest
import tempfile
import os
import shutil
import dbus.service
import nodlehs.bus
from nodlehs.storage import *
from nodlehs.objects import File
from dulwich.objects import *

class TestStorage(unittest.TestCase):

    def setUp(self):
        nodlehs.bus.init()
        self.repo_path = tempfile.mktemp()
        os.mkdir(self.repo_path)
        self.storage = Storage.init_bare(nodlehs.bus.init().busname, self.repo_path)
        self.branch = Branch(self.storage, "master")

    def test_Branch_root(self):
        self.assert_(self.branch.root is not None)
        self.assert_(self.branch.root is self.branch.record.root)
        self.assert_(self.branch.root is self.branch._next_record.root)

    def tearDown(self):
        shutil.rmtree(self.repo_path)

if __name__ == '__main__':
    unittest.main()
