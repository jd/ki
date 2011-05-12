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
import time

def _edit_with_tempfile(s, suffix=""):
    tmpf = tempfile.mktemp() + suffix
    with file(tmpf, "w") as f:
        f.write(s)
    if os.system("%s %s" % (os.getenv("EDITOR"), tmpf)) == 0:
        with file(tmpf, "r") as f:
            s = f.read()
    else:
        s = None
    try:
        os.unlink(tmpf)
    except:
        pass
    return s


def commit(box, **kwargs):
    box_path = storage.GetBox(box)
    bus.get_object(nodlehs.storage.BUS_INTERFACE, box_path).Commit()


def mount(box, mountpoint, **kwargs):
    box_path = storage.GetBox(box)
    bus.get_object(nodlehs.storage.BUS_INTERFACE, box_path).Mount(mountpoint)


def recordlist(box, **kwargs):
    box_path = storage.GetBox(box)
    for (sha, commit_time) in bus.get_object(nodlehs.storage.BUS_INTERFACE, box_path).RecordList():
        print "%s %s" % (sha, time.strftime("%a, %d %b %Y %H:%M:%S %z", time.localtime(commit_time)))


def _config(obj, what):
    if what == 'set':
        obj.SetConfig(sys.stdin.read())
    elif what == 'edit':
        obj.SetConfig(_edit_with_tempfile(obj.GetConfig(), ".js"))
    else:
        print obj.GetConfig()

def config(what, **kwargs):
    _config(storage, what)


def remote_add(name, url, weight, **kwargs):
    storage.AddRemote(name, url, weight)


def remote_remove(name, **kwargs):
    storage.RemoveRemote(name)


def remote_list(**kwargs):
    for item in storage.ListRemotes():
        r = bus.get_object(nodlehs.storage.BUS_INTERFACE, item)
        print "%s:" % r.GetName()
        print "    ID: %s" % r.GetID()
        print "    URL: %s" % r.GetURL()
        print "    Weight: %d" % r.GetWeight()


def _remote_name_to_obj(name):
    return bus.get_object(nodlehs.storage.BUS_INTERFACE,
                       "%s/remotes/%s" % (storage.__dbus_object_path__, name))


def remote_showrefs(name, **kwargs):
    r = _remote_name_to_obj(name)
    for ref, sha in r.GetRefs().iteritems():
        print "        %30s %s" % (ref[-30:], sha)


def remote_config(what, name, **kwargs):
    _config(_remote_name_to_obj(name), what)

def info(**kwargs):
    print "Path: %s" % storage.GetPath()
    print "ID: %s" % storage.GetID()

parser = argparse.ArgumentParser()
parser.add_argument('--storage', type=str,
                    help='Storage path.')
subparsers = parser.add_subparsers(help='Action to perform.',
                                   title="Actions",
                                   description="Actions to perform on the given box.")

# Info
parser_info = subparsers.add_parser('info', help='Show storage information.')
parser_info.set_defaults(action=info)
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
# Mount
parser_recordlist = subparsers.add_parser('recordlist',
                                          help='Show the list of records of a box.')
parser_recordlist.set_defaults(action=recordlist)
parser_recordlist.add_argument('box', type=str,
                               help='The box to show records list of.')
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
## Showrefs
parser_remote_showrefs = subparsers_remote.add_parser('showrefs', help='List remote refs.')
parser_remote_showrefs.add_argument('name', type=str,
                                    help='Remote name.')
parser_remote_showrefs.set_defaults(action=remote_showrefs)
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
parser_remote_remove.add_argument('name', type=str,
                                  help='Remote name.')
## Config
parser_remote_config = subparsers_remote.add_parser('config', help='Remote config access.')
parser_remote_config.set_defaults(action=remote_config)
parser_remote_config.add_argument('what', type=str, choices=['dump', 'set', 'edit'],
                           help='The action to perform.')
parser_remote_config.add_argument('name', type=str, help='Remote name.')


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

