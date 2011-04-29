#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# nodlehs.dbus -- D-Bus communication with Nodlehs
#
#    Copyright © 2011  Julien Danjou <julien@danjou.info>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import dbus
import dbus.service
# Hack for the interface code
from dbus.service import Object as dbusObject
import dbus.mainloop.glib

from .constants import *
from .storage import *
from .utils import Singleton


_storage_manager = None


def get_storage_manager(bus):
    global _storage_manager
    if _storage_manager is None:
        _storage_manager = StorageManager(bus)
    return _storage_manager


def init():
    dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
    return get_storage_manager(dbus.service.BusName(BUS_INTERFACE, dbus.SessionBus()))


class StorageManager(dbus.service.Object):

    def __init__(self, busname):
        # XXX Singleton?
        self.storages = {}
        self.busname = busname
        super(StorageManager, self).__init__(busname, "%s/%s" % (BUS_PATH,
                                                                 self.__class__.__name__))

    @dbus.service.method(dbus_interface=BUS_INTERFACE,
                         in_signature='s', out_signature='o')
    def CreateStorage(self, root):
        if not self.storages.has_key(repo):
            self.storages[repo] = Storage(self.busname, root)
        return self.storages[repo].__dbus_object_path__




