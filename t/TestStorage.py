#!/usr/bin/env python

import unittest
import tempfile
import os
import shutil
import dbus.mainloop.glib
import dbus.service
import nodlehs.bus
from nodlehs.storage import *
from nodlehs.objects import File
from dulwich.objects import *

def init_storage(self):
    dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
    dbus.service.BusName(BUS_INTERFACE, dbus.SessionBus())
    self.repo_path = tempfile.mktemp()
    os.mkdir(self.repo_path)
    self.storage = Storage.init_bare(dbus.service.BusName(BUS_INTERFACE, dbus.SessionBus()), self.repo_path)
    self.box = Box(self.storage, "master", create=True)

class TestStorage(unittest.TestCase):

    def setUp(self):
        init_storage(self)

    def test_Box_root(self):
        self.assert_(self.box.root is not None)
        self.assert_(self.box.root is self.box.record.root)
        self.assert_(self.box.root is self.box._next_record.root)

    def tearDown(self):
        shutil.rmtree(self.repo_path)

if __name__ == '__main__':
    unittest.main()
