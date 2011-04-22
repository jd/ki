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

from dulwich.client import get_transport_and_path

class FetchError(Exception):
    pass

def progress(x):
    print "FETCH PROGRESS: ",
    print x

class Remote(object):

    def __init__(self, storage, url):
        self.url = url
        self.storage = storage
        self.client, self.path = get_transport_and_path(url)

    def fetch_sha1s(self, sha1s):
        try:
            self.client.fetch(self.path, self.storage, lambda refs: sha1s, progress)
        except IOError:
            raise FetchError

    def push(self, refs):
        self.client.send_pack(self.path,
                              lambda oldrefs: refs,
                              self.storage.object_store.generate_pack_contents)
