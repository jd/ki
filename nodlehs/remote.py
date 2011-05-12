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

import threading
from .config import Configurable, Config, BUS_INTERFACE
from .objects import File
from dulwich.client import get_transport_and_path
import dbus.service
import uuid


class FetchError(Exception):
    pass


class Remote(dbus.service.Object, Configurable):

    _id_ref = "refs/tags/id"

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

    @dbus.service.method(dbus_interface="%s.Remote" % BUS_INTERFACE,
                         out_signature='s')
    def GetID(self):
        return self.id

    def fetch_sha1s(self, sha1s):
        return self.fetch(lambda refs: sha1s)

    @property
    def id(self):
        """Fetch remote id."""
        try:
            return self._id
        except AttributeError:
            try:
                self._id = str(self.storage[self.refs[Remote._id_ref]])
            except KeyError:
                f = File(self.storage)
                f.write(str(uuid.uuid4()))
                def determine_wants(self, refs):
                    newrefs = refs.copy()
                    refs[Remote._id_refs] = f.store()
                    return f
                self.push(determine_wants)
                self._id = str(f)
        return self._id

    @property
    def config(self):
        """Fetch configuration from the remote."""
        try:
            return Config(self.storage, self.on_config_store, self.storage[self.refs[Config.ref]])
        except KeyError:
            return Config(self.storage, self.on_config_store)

    def on_config_store(self, sha1):
        """Store the config on the remote."""
        def determine_wants(oldrefs):
            newrefs = oldrefs.copy()
            newrefs[Config.ref] = sha1
            return newrefs
        self.push(determine_wants)

    @property
    def refs(self):
        """Connect to the remote and returns all the refs it has."""
        return self.fetch(lambda refs: [])

    @dbus.service.signal(dbus_interface="%s.Remote" % BUS_INTERFACE,
                         signature='as')
    def FetchProgress(self, status):
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
        return self.client.send_pack(self.path,
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


class Syncer(threading.Thread):

    def __init__(self, storage):
        self.storage = storage
        super(Syncer, self).__init__()
        self.daemon = True
        self.start()

    def run(self):
        while True:
            # XXX configure timeout
            print "WAIT"
            self.storage.must_be_sync.wait(30)
            print "END WAIT"
            if self.storage.must_be_sync.is_set():
                print "IS SET -> PUSH"
                self.storage.push()
                self.storage.must_be_sync.clear()
            else:
                print "NO SET"
                # Timeout
                # XXX fetch
                pass
