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


def commit(commit, **kwargs):
    branch.Commit()


def mount(mountpoint, **kwargs):
    branch.Mount(mountpoint[0])

parser = argparse.ArgumentParser()
parser.add_argument('--storage', type=str,
                    help='a storage path')
parser.add_argument('--branch', type=str,
                    required=True,
                    help='the branch to use')
subparsers = parser.add_subparsers(help='Action to perform.',
                                   title="Actions",
                                   description="Actions to perform on the given branch.")

# Mount
parser_mount = subparsers.add_parser('mount', help='Mount the branch.')
parser_mount.set_defaults(action=mount)
parser_mount.add_argument('mountpoint',
                          type=str, nargs=1,
                          help='Them ountpoint.')

# Commit
parser_mount = subparsers.add_parser('commit', help='Commit the branch immediately.')
parser_mount.set_defaults(action=commit)

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
branch_path = storage.GetBranch(args.branch)
branch = bus.get_object(nodlehs.storage.BUS_INTERFACE, branch_path)

args.action(**args.__dict__)
