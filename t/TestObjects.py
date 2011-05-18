#!/usr/bin/env python

import unittest
import tempfile
import os
import shutil
import dbus.service
from nodlehs.storage import Storage
from nodlehs.objects import *
from dulwich.objects import *

from TestStorage import TestUsingStorage

class TestObjects(TestUsingStorage):

    def test_Storable_init(self):
        r = Record(self.storage)
        Storable(self.storage, r.store())
        Storable(self.storage, r.object)
        Storable(self.storage, r)
        self.assertRaises(BadObjectType, Storable, self.storage, 123)

    def test_Storable_copy(self):
        r = Record(self.storage)
        s = Record(self.storage, r.store())
        scopy = s.copy()
        self.assert_(scopy == s)
        self.assert_(scopy is not s)

    def test_Storable_len(self):
        r = Record(self.storage)
        self.assert_(len(r))

    def test_Storable_eq(self):
        self.assert_(Symlink(self.storage, None, "/dtc") == Symlink(self.storage, None, "/dtc"))

    def test_Storable_hash(self):
        self.assert_(set([Symlink(self.storage, None, "/dtc")]) == set([Symlink(self.storage, None, "/dtc")]))

    def test_make_object(self):
        self.assertRaises(BadObjectType, make_object, self.storage, 0, "")
        self.assert_(isinstance(make_object(self.storage, S_IFGITLINK, File(self.storage).store()), File))
        self.assert_(isinstance(make_object(self.storage, stat.S_IFDIR, Directory(self.storage).store()), Directory))
        self.assertRaises(BadObjectType, make_object,
                          self.storage, stat.S_IFDIR, File(self.storage).store())
        self.assertRaises(BadObjectType, make_object,
                          self.storage, S_IFGITLINK, Record(self.storage).store())

    def test_Record_find_common_ancestor(self):
        r1 = Record(self.storage)
        r2 = Record(self.storage)
        r1.root["a"] = (100644, File(self.storage))
        r2.root["b"] = (100644, File(self.storage))
        r2.parents.add(r1)
        self.assert_(r2.find_common_ancestors(r1) is None)
        self.assert_(r1.find_common_ancestors(r1) is None)
        r3 = Record(self.storage)
        r3.root["c"] = (100644, File(self.storage))
        r3.parents.add(r1)
        ret = r3.find_common_ancestors(r2)
        self.assert_(len(ret) is 1)
        self.assert_(ret.pop() is r1)
        ret = r2.find_common_ancestors(r3)
        self.assert_(len(ret) is 1)
        self.assert_(ret.pop() is r1)
        r4 = Record(self.storage)
        r4.root["d"] = (100644, File(self.storage))
        r4.parents.add(r2)
        r4.parents.add(r3)
        r5 = Record(self.storage)
        r5.root["e"] = (100644, File(self.storage))
        r5.parents.add(r2)
        r5.parents.add(r3)
        ret = r4.find_common_ancestors(r5)
        self.assert_(len(ret) is 2)
        self.assert_(ret == set([r2, r3]))
        r6 = Record(self.storage)
        r6.parents.add(r2)
        self.assert_(r6.find_common_ancestors(r5) == set([ r2 ]))
        self.assert_(r5.find_common_ancestors(r6) == set([ r2 ]))
        self.assert_(r2.find_common_ancestors(r6) == set([ r1 ]))
        self.assert_(r6.find_common_ancestors(r2) == set([ r1 ]))
        r6copy = Record(self.storage, r6.store())
        self.assert_(r6copy.find_common_ancestors(r5) == set([ r2 ]))

    def test_Record_intervals(self):
        r1 = Record(self.storage)
        r2 = Record(self.storage)
        r3 = Record(self.storage)
        r1.parents.append(r2)
        r2.parents.append(r3)
        self.assert_(r3.commit_intervals(r1) == None)
        self.assert_(r2.commit_intervals(r1) == None)
        self.assert_(r1.commit_intervals(r3) == [ set([ r2 ] )])

    def test_Record_is_child_of(self):
        r1 = Record(self.storage)
        r2 = Record(self.storage)
        r3 = Record(self.storage)
        r1.parents.append(r2)
        r2.parents.append(r3)
        self.assert_(not r3.is_child_of(r1))
        self.assert_(r1.is_child_of(r2))
        self.assert_(r1.is_child_of(r3))

    def test_Record_operators(self):
        r1 = Record(self.storage)
        r2 = Record(self.storage)
        r3 = Record(self.storage)
        r1.parents.append(r2)
        r2.parents.append(r3)
        self.assert_(r1 > r2)
        self.assert_(r1 <= r1)
        self.assert_(r3 < r1)
        self.assert_(r3 >= r3)
        self.assert_(r3 < r2)

    def test_Directory_list_blobs(self):
        d = Directory(self.storage)
        f1 = File(self.storage)
        f1.data = "some data"
        f2 = File(self.storage)
        f2.data = "data"
        f3 = File(self.storage)
        f3.data = "did I write some data already"
        d["arf/bla.txt"] = (644, f1)
        d["arf/kikoo.txt"] = (644, f2)
        d["arf/bla/bla.txt"] = (644, f3)
        self.assert_(len(d.list_blobs()) == 0)
        self.assert_(d["arf"][1].list_blobs() == set([ f1.id() ]))
        self.assert_(d["arf/bla"][1].list_blobs() == set([ f3.id() ]))

    def test_Directory_list_blobs_recursive(self):
        d = Directory(self.storage)
        f1 = File(self.storage)
        f1.data = "some data"
        f2 = File(self.storage)
        f2.data = "data"
        f3 = File(self.storage)
        f3.data = "did I write some data already"
        d["arf/bla.txt"] = (644, f1)
        d["arf/kikoo.txt"] = (644, f2)
        d["arf/toto.txt"] = (644, f3)
        d["arf/bla/bla.txt"] = (644, f3)
        d["arf/hep/file.txt"] = (644, f1)
        self.assert_(d.list_blobs_recursive() == set([ f1.id(), f2.id(), f3.id() ]))

    def test_Directory_mkdir(self):
        directory = Directory(self.storage)
        directory.mkdir("a/b/c")
        self.assert_(isinstance(directory["a"][1], Directory))
        self.assert_(isinstance(directory["a"][1]["b"][1], Directory))
        self.assert_(isinstance(directory["a"][1]["b"][1]["c"][1], Directory))

    def test_Directory_add(self):
        directory = Directory(self.storage, Tree())
        f = File(self.storage)
        directory["m/k/x"] = (0, f)
        self.assert_(isinstance(directory["m"][1], Directory))
        self.assert_(isinstance(directory["m"][1]["k"][1], Directory))
        self.assert_(isinstance(directory["m"][1]["k"][1]["x"][1], File))
        self.assert_(directory["m"][1]["k"][1]["x"][1] is f)

    def test_Directory_del(self):
        directory = Directory(self.storage, Tree())
        f = File(self.storage)
        directory["m/k/x"] = (0, f)
        del directory["m/k"]
        self.assert_(isinstance(directory["m"][1], Directory))
        self.assertRaises(NoChild, lambda: directory["m"][1]["k"])

    def test_File_operations(self):
        f = File(self.storage)
        f.write("abc")
        f.seek(0)
        self.assert_(f.read() == "abc")
        self.assert_(len(f) == 3)
        f.truncate(0)
        self.assert_(f.read() == "")

    def test_File_merge(self):
        base = File(self.storage)
        base.write("hello\nworld\nhow are you?\n")
        f = File(self.storage)
        f.write("hello\nyou\nhow are you?\n")
        other = File(self.storage)
        other.write("hello\nworld\nwhere are you?\n")
        self.assertRaises(MergeConflictError, f.merge, str(base), str(other))

    def test_Symlink_target(self):
        s = Symlink(self.storage, None, "/")
        self.assert_(s.target == "/")
        s.target = "/stuff"
        self.assert_(s.target == "/stuff")

if __name__ == '__main__':
    unittest.main()
