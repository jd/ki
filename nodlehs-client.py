#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# nodlehs-client -- Distributed file system client
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

import sys
import os
import dbus
import nodlehs.storage
import argparse
import tempfile

def commit(box, **kwargs):
    box_path = storage.GetBox(box)
    bus.get_object(nodlehs.storage.BUS_INTERFACE, box_path).Commit()


def mount(box, mountpoint, **kwargs):
    box_path = storage.GetBox(box)
    bus.get_object(nodlehs.storage.BUS_INTERFACE, box_path).Mount(mountpoint)


def remote_add(name, url, weight, **kwargs):
    storage.AddRemote(name, url, weight)


def remote_remove(name, **kwargs):
    storage.RemoveRemote(name)


def remote_list(**kwargs):
    for item in storage.ListRemotes():
        r = bus.get_object(nodlehs.storage.BUS_INTERFACE, item)
        print "%s:" % r.GetName()
        print "    URL: %s" % r.GetURL()
        print "    Weight: %d" % r.GetWeight()
        print "    Refs:"
        for ref, sha in r.GetRefs().iteritems():
            print "        %30s %s" % (ref[-30:], sha)


def config(what, **kwargs):
    if what == 'set':
        storage.SetConfig(sys.stdin.read())
    elif what == 'edit':
        tmpf = tempfile.mktemp()
        with file(tmpf, "w") as f:
            f.write(storage.GetConfig())
        ret = os.system("%s %s" % (os.getenv("EDITOR"), tmpf))
        if ret == 0:
            with file(tmpf, "r") as f:
                storage.SetConfig(f.read())
        try:
            os.unlink(tmpf)
        except:
            pass
    else:
        print storage.GetConfig()


parser = argparse.ArgumentParser()
parser.add_argument('--storage', type=str,
                    help='Storage path.')
subparsers = parser.add_subparsers(help='Action to perform.',
                                   title="Actions",
                                   description="Actions to perform on the given box.")

# Config
parser_config = subparsers.add_parser('config', help='Dump or set storage configuration.')
parser_config.set_defaults(action=config)
parser_config.add_argument('what', type=str, choices=['dump', 'set', 'edit'],
                           help='The action to perform.')
# Mount
parser_mount = subparsers.add_parser('mount', help='Mount the box.')
parser_mount.set_defaults(action=mount)
parser_mount.add_argument('box', type=str,
                          help='The box to mount.')
parser_mount.add_argument('mountpoint',
                          type=str,
                          help='The mountpoint.')
# Commit
parser_mount = subparsers.add_parser('commit', help='Commit the box immediately.')
parser_mount.set_defaults(action=commit)
parser_mount.add_argument('box', type=str,
                          help='The box to commit.')

# Remotes
parser_remote = subparsers.add_parser('remote', help='Act on remotes.')
subparsers_remote = parser_remote.add_subparsers(help='Action to perform on remotes.',
                                                 title='Actions',
                                                 description='Action to perform on the given remote of a storage.')
## List
parser_remote_list = subparsers_remote.add_parser('list', help='List remotes.')
parser_remote_list.set_defaults(action=remote_list)
## Add
parser_remote_add = subparsers_remote.add_parser('add', help='Add a remote.')
parser_remote_add.set_defaults(action=remote_add)
parser_remote_add.add_argument('name', type=str,
                               help='Remote name.')
parser_remote_add.add_argument('url', type=str,
                               help='Remote URL.')
parser_remote_add.add_argument('weight', type=int,
                               default=100, nargs='?',
                               help='Remote weight.')
## Remove
parser_remote_remove = subparsers_remote.add_parser('remove', help='Remove a remote.')
parser_remote_remove.set_defaults(action=remote_remove)
parser_remote_remove.add_argument('url', type=str,
                                  help='Remote URL.')

args = parser.parse_args()

# Now connect.
bus = dbus.SessionBus()
storage_manager = bus.get_object(nodlehs.storage.BUS_INTERFACE,
                                 "%s/StorageManager" % nodlehs.storage.BUS_PATH)
if args.storage is not None:
    storage_path = storage_manager.GetStorage(args.storage)
else:
    storage_path = storage_manager.GetUserStorage()
storage = bus.get_object(nodlehs.storage.BUS_INTERFACE, storage_path)

args.action(**args.__dict__)
