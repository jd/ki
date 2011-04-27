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

from .constants import *
from .storage import *


def init():
    dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
    StorageManager(dbus.service.BusName(BUS_INTERFACE, dbus.SessionBus()))


class StorageManager(dbus.service.Object):

    def __init__(self, busname):
        self.storages = {}
        self.busname = busname
        super(StorageManager, self).__init__(busname, BUS_PATH)

    @dbus.service.method(dbus_interface=BUS_INTERFACE,
                         in_signature='ss', out_signature='o')
    def CreateStorage(self, repo, mountpoint):
        if not self.storages.has_key(repo):
            self.storages[repo] = Storage(repo, mountpoint, self.busname)
        return self.storages[repo].__dbus_object_path__




