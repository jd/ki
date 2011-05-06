#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# nodlehs.remote -- Git based file system storage remote access
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

from .config import Configurable, Config, BUS_INTERFACE
from dulwich.client import get_transport_and_path
import dbus.service


class FetchError(Exception):
    pass


class Remote(dbus.service.Object, Configurable):

    def __init__(self, storage, name, url, weight=100):
        self.url = url
        self.weight = weight
        self.storage = storage
        self.name = name
        self.client, self.path = get_transport_and_path(url)
        super(Remote, self).__init__(storage.bus,
                                     "%s/remotes/%s" % (storage.__dbus_object_path__, name))

    @dbus.service.method(dbus_interface="%s.Remote" % BUS_INTERFACE,
                         out_signature='s')
    def GetURL(self):
        return self.url

    @dbus.service.method(dbus_interface="%s.Remote" % BUS_INTERFACE,
                         out_signature='s')
    def GetName(self):
        return self.name

    @dbus.service.method(dbus_interface="%s.Remote" % BUS_INTERFACE,
                         out_signature='a{ss}')
    def GetRefs(self):
        return self.refs

    @dbus.service.method(dbus_interface="%s.Remote" % BUS_INTERFACE,
                         out_signature='i')
    def GetWeight(self):
        return self.weight

    def fetch_sha1s(self, sha1s):
        try:
            self.fetch(lambda refs: sha1s)
        except IOError:
            raise FetchError(sha1s)

    @property
    def config(self):
        """Fetch configuration from the remote."""
        try:
            return Config(self.storage, self.on_config_store, self.storage[self.refs[Config.ref]])
        except KeyError:
            return Config(self.storage, self.on_config_store)

    def on_config_store(self, sha1):
        """Store the config on the remote."""
        self.push(lambda oldrefs: { Config.ref: sha1 })

    @property
    def refs(self):
        """Connect to the remote and returns all the refs it has."""
        return self.fetch(lambda refs: [])

    @dbus.service.signal(dbus_interface="%s.Remote" % BUS_INTERFACE,
                         signature='ass')
    def FetchProgress(self, sha1, status):
        pass

    def fetch(self, determine_wants):
        """Fetch data from the remote.
        The function passed in determine_wats is called with the refs dict as first and only argument:
        { "refs/heads/master": "08a1c9f9742bcbd27c44fb84b662c68fabd995e1",
        … }
        The determine_wants function should returns a list of SHA1 to fetch."""
        return self.client.fetch(self.path, self.storage, determine_wants, self.FetchProgress)

    def push(self, determine_wants):
        """Push data to the remote.
        The function passed in determine_wants is called with the refs dict as first and only argument:
        { "refs/heads/master": "08a1c9f9742bcbd27c44fb84b662c68fabd995e1",
        … } """
        self.client.send_pack(self.path,
                              determine_wants,
                              self.storage.object_store.generate_pack_contents)

    def __le__(self, other):
        if isinstance(other, Remote):
            return self.weight <= other.weight
        return self.weight <= other

    def __lt__(self, other):
        if isinstance(other, Remote):
            return self.weight < other.weight
        return self.weight < other

    def __ge__(self, other):
        if isinstance(other, Remote):
            return self.weight >= other.weight
        return self.weight >= other

    def __gt__(self, other):
        if isinstance(other, Remote):
            return self.weight > other.weight
        return self.weight > other

