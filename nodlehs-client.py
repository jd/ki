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
import dbus
import nodlehs.storage
import argparse


def commit(branch, **kwargs):
    branch_path = storage.GetBranch(branch)
    bus.get_object(nodlehs.storage.BUS_INTERFACE, branch_path).Commit()


def mount(branch, mountpoint, **kwargs):
    branch_path = storage.GetBranch(branch)
    bus.get_object(nodlehs.storage.BUS_INTERFACE, branch_path).Mount(mountpoint)


def remotes_add(url, weight, **kwargs):
    storage.AddRemote(url, weight)


def remotes_remove(url, **kwargs):
    storage.RemoveRemote(url)


def remotes_list(**kwargs):
    print "Weight │ URL"
    print "───────┼────"
    for item in storage.ListRemotes():
        print "%6d" % item[1],
        print "│",
        print "%s" % item[0]


def config(key, value, **kwargs):
    if key is None:
        for key in storage.ListConfigKeys():
            print key
    elif value is not None:
        storage.SetConfig(key, value)
    else:
        print storage.GetConfig(key)


parser = argparse.ArgumentParser()
parser.add_argument('--storage', type=str,
                    help='Storage path.')
subparsers = parser.add_subparsers(help='Action to perform.',
                                   title="Actions",
                                   description="Actions to perform on the given branch.")

# Setprefetch
parser_config = subparsers.add_parser('config', help='Set or get configuration parameters.')
parser_config.set_defaults(action=config)
parser_config.add_argument('key', type=str, nargs='?',
                           help='The configuration key to set.')
parser_config.add_argument('value', type=str,
                           nargs='?',
                           help='The configuration value to set.')

# Mount
parser_mount = subparsers.add_parser('mount', help='Mount the branch.')
parser_mount.set_defaults(action=mount)
parser_mount.add_argument('branch', type=str,
                          help='The branch to mount.')
parser_mount.add_argument('mountpoint',
                          type=str,
                          help='The mountpoint.')
# Commit
parser_mount = subparsers.add_parser('commit', help='Commit the branch immediately.')
parser_mount.set_defaults(action=commit)
parser_mount.add_argument('branch', type=str,
                          help='The branch to commit.')

# Remotes
parser_remotes = subparsers.add_parser('remotes', help='List the remotes.')
subparsers_remotes = parser_remotes.add_subparsers(help='Action to perform on remotes.',
                                                   title='Actions',
                                                   description='Action to perform on the given remote of a storage.')
## List
parser_remotes_list = subparsers_remotes.add_parser('list', help='List remotes.')
parser_remotes_list.set_defaults(action=remotes_list)
## Add
parser_remotes_add = subparsers_remotes.add_parser('add', help='Add a remote.')
parser_remotes_add.set_defaults(action=remotes_add)
parser_remotes_add.add_argument('url', type=str,
                                help='Remote URL.')
parser_remotes_add.add_argument('weight', type=int,
                                default=100,
                                help='Remote URL weight.')
## Remove
parser_remotes_remove = subparsers_remotes.add_parser('remove', help='Remove a remote.')
parser_remotes_remove.set_defaults(action=remotes_remove)
parser_remotes_remove.add_argument('url', type=str,
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
