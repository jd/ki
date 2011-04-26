#!/usr/bin/env python

import unittest
from nodlehs.merge import *

class TestMerge(unittest.TestCase):

    def test_merge_conflict(self):
        try:
            merge("hello world", "hello there", "hello you")
        except MergeConflictError as e:
            self.assert_(e.number_of_conflicts == 1)
        else:
            self.assert_(False)

    def test_merge_ok(self):
        self.assert_(merge("hello\nyou", "hello", "hello") == "hello\nyou")
        self.assert_(merge("hello\nyou", "hello", "hello\nyou") == "hello\nyou")

    def test_merge_binary(self):
        data ="89504E470D0A1A0A0000000D494844520000000F0000000A08060000006B1B04F9000000017352474200AECE1CE900000006624B474400FF00FF00FFA0BDA793000000097048597300000B1100000B11017F645F910000000774494D4507DA06090A010C9003D77F000001894944415428CF5D903B6F13511085BFEBBDF23E12E3470805E2D1381205A22015250D75247A0A7E582A7E0350468212A20809A4F0520441C606DB3864BD8FBB7786E22E1162A4334767A4A3333365AD77AE75F0A2882A2A202A8810B482880456454431069E1E9E61B73661F766C4FFA52D42FB4703D6C0D149819D2C1DABC2B211050FA8826F53BD1A2A0FEFBE951C7E3EC30E33C3C7D30507C79EBD7B97B9D2EF503AC589C109340ACE2B620C935F9E27CFBF32BEAA6499C5826177678BDBD7D7EC1FC632EE8E53D2C4125943EE94552EBC7EBFE2CD87390FEF6F930E079C3C9B60BD844BA238E3F1838C17AF8E79FBE912D33C66FE5BE8463591E46C0FE0D1DE0E4503EB2A6C63A535578D72FA7DC6787C835B498A13A85A941E26D305D31F73E2DE088709E6469406982D57A4BD211A75C96BA592F09CCA43ED95A437C2AD4B16CB25F16014CCF7A49B7D6A31D4024EC3B36A0127E0C45009D04D88D4729E17C17CF4A5A263134AB7BE48090877D55E5B86CACBC57CF6B3E00FA3AE023C3B0CE5D50000000049454E44".decode('hex')
        try:
            merge("bla", data, "bli")
        except MergeBinaryError:
            pass
        else:
            self.assert_(False)



        self.assert_(merge("hello\nyou", "hello", "hello\nyou") == "hello\nyou")


if __name__ == '__main__':
    unittest.main()
