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

from .objects import Config
from .utils import dbus_uuid, dbus_clean_name
from dulwich.client import get_transport_and_path
import dbus.service

BUS_INTERFACE = "org.naquadah.Nodlehs"

class FetchError(Exception):
    pass

def progress(x):
    print "FETCH PROGRESS: ",
    print x

class Remote(dbus.service.Object):

    def __init__(self, storage, url):
        self.url = url
        self.storage = storage
        self.client, self.path = get_transport_and_path(url)
        super(Remote, self).__init__(storage.bus,
                                     "%s/remote_%s_%s" % (storage.__dbus_object_path__,
                                                          dbus_clean_name(self.url),
                                                          dbus_uuid()))

    @dbus.service.method(dbus_interface="%s.Remote" % BUS_INTERFACE,
                         out_signature='s')
    def GetURL(self):
        return self.url

    @dbus.service.method(dbus_interface="%s.Remote" % BUS_INTERFACE,
                         out_signature='a{ss}')
    def GetRefs(self):
        return self.refs

    def fetch_sha1s(self, sha1s):
        try:
            self.client.fetch(self.path, self.storage, lambda refs: sha1s, progress)
        except IOError:
            raise FetchError(sha1s)

    def _config_read_remote_refs(self, oldrefs):
        """Callback function used when getting config from the remote.

        This is used to determine the "wants" while fetching. We try to
        fetch the ref named Config.ref if it exists, and store its SHA1 for
        future use in self._config_sha. Otherwise we just return an empty
        list."""
        try:
            self._config_sha = oldrefs[Config.ref]
        except KeyError:
            # No config in remote :(
            self._config_sha = None
            return []
        return [ self._config_sha ]

    @property
    def config(self):
        # Fetch configuration from the remote.
        self.client.fetch(self.path, self.storage, self._config_read_remote_refs)
        return Config.from_sha1(self.storage, self._config_sha)

    @property
    def refs(self):
        """Connect to the remote and returns all the refs it has."""
        return self.client.fetch(self.path, self.storage, lambda refs: [])

    def push(self, determine_wants):
        """Push data to the remote.
        The function passed in determine_wants is called with the refs dict as first and only argument:
        { "refs/heads/master": "08a1c9f9742bcbd27c44fb84b662c68fabd995e1",
          … } """
        self.client.send_pack(self.path,
                              determine_wants,
                              self.storage.object_store.generate_pack_contents)
