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


class TestUsingStorage(unittest.TestCase):

    def make_temp_storage(self):
        repo_path = tempfile.mktemp()
        os.mkdir(repo_path)
        return Storage.init_bare(self.bus, repo_path)

    def setUp(self):
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        dbus.service.BusName(BUS_INTERFACE, dbus.SessionBus())
        self.bus = dbus.service.BusName(BUS_INTERFACE, dbus.SessionBus())
        self.storage = self.make_temp_storage()
        self.box = Box(self.storage, "master", create=True)

    def tearDown(self):
        shutil.rmtree(self.storage.path)


class TestStorage(TestUsingStorage):

    def test_Storage_id(self):
        self.assert_(isinstance(self.storage.id, str))
        self.assert_(len(self.storage.id) == 36)

    def test_Storage_config(self):
        self.assert_(isinstance(self.storage.config, Config))

    def test_Box_root(self):
        self.assert_(self.box.root is not None)
        self.assert_(self.box.root is self.box.record.root)
        self.assert_(self.box.root is self.box._next_record.root)

if __name__ == '__main__':
    unittest.main()
