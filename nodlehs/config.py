#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# nodlehs.config -- Configuration handling
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

import json
from .objects import File, Storable
import dbus.service

BUS_INTERFACE = "org.naquadah.Nodlehs"


class Config(File):
    """A configuration based on JSON."""

    ref = 'refs/tags/config'

    # Default configuration
    # XXX validate with JSON schema ?
    # https://github.com/sunlightlabs/validictory
    _default_config = { "boxes" : {} }

    def __init__(self, storage, on_store, obj=None):
        super(Config, self).__init__(storage, obj)
        self.on_store = on_store
        if obj is None:
            self._config = self._default_config
            self._update(Config.__init__)

    def load_json(self, value):
        """Load JSON data."""
        self._config = json.loads(value)
        self.store()

    def __getitem__(self, key):
        return self._config[key]

    def __setitem__(self, key, value):
        self._config[key] = value
        self.store()

    def _update(self, operation_type):
        self.truncate(0)
        json.dump(self._config, self, indent=4)
        super(Config, self)._update(operation_type)

    def store(self):
        """Store the config in the storage."""
        # Bypass File.store.
        # Not sure my programming teacher would like it.
        oid = Storable.store(self)
        self.on_store(oid)
        return oid


class Configurable(object):

    def on_config_store(self, sha1):
        raise NotImplementedError

    @dbus.service.method(dbus_interface="%s.Config" % BUS_INTERFACE,
                         out_signature='s')
    def GetConfig(self):
        return str(self.config)

    @dbus.service.method(dbus_interface="%s.Config" % BUS_INTERFACE,
                         in_signature='s')
    def SetConfig(self, conf):
        self.config.load_json(conf)
