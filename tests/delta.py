#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for delta functions."""

from unittest import TestCase
from hiispider import delta

import os
import random
import simplejson

srt = lambda l: list(sorted(l))
datapath = os.path.join(os.path.dirname(__file__), 'deltas')

def read(path):
    """Read a file and return its contents, cleaning up after file handles."""
    with open(path) as f:
        contents = f.read()
    return contents

class CompareListsTest(TestCase):
    """Test delta._compare_lists."""
    def test_basics(self):
        """Basic list comparisson tests."""
        comp = delta._compare_lists
        # test the identity comparisson against an empty list
        for i in range(10):
            rand = srt(random.sample(xrange(10000), 100))
            self.assertEqual(srt(comp(rand, [])), rand)
        # test some known valued lists against eachother
        self.assertEqual(comp([1,2,3,4], [2,3,4]), [1])
        # going the other way doesn't get us anything
        self.assertEqual(comp([2,3,4], [1,2,3,4]), [])
        # test interpolated values
        self.assertEqual(comp([1,2,3,4,5,6,7], [2,5,1,3]), [4,6,7])
        # establish what duplicates mean
        self.assertEqual(comp([1,1,2,3,4,4,5], [1,2,3,4,5]), [])
        # test comparissons on composite types
        self.assertEqual(srt(comp(
            [{'a': 1, 'b': 2}, {'c': 3}, ['foo', 'bar']],
            [{'c': 3}])),
            srt([{'a': 1, 'b': 2}, ['foo', 'bar']]),
        )
        self.assertEqual(srt(comp(
            [{'a': 1, 'b': 2}, {'c': 3}, ['foo', 'bar']],
            [{'a': 1, 'b': 3}])),
            srt([{'a': 1, 'b': 2}, {'c': 3}, ['foo', 'bar']]),
        )

class CompareDictsTest(TestCase):
    """Test delta._compare_dicts."""
    def test_basics(self):
        """Basic dict comparisson tests."""
        comp = delta._compare_dicts
        # test the identify comparisson against an empty dict
        for i in range(10):
            keys = [''.join(random.sample('abcdefghijklmnopqrstuvwxyz', 3)) for x in range(100)]
            values = random.sample(xrange(10000), 100)
            d = dict(zip(keys, values))
            # compare_dicts returns a list of maps of new-keys to new values
            self.assertEqual(srt(comp(d, {})), srt([{k:v} for k,v in d.iteritems()]))

    def test_corpus(self):
        """Use tests/deltas/ test case data."""
        print ''
        full = lambda x: os.path.join(datapath, x)
        comp = delta._compare_dicts
        old = [read(full(f)).decode('base64') for f in sorted(os.listdir(datapath)) if f.endswith('old.js')]
        new = [read(full(f)).decode('base64') for f in sorted(os.listdir(datapath)) if f.endswith('new.js')]
        res = [read(full(f)).decode('base64') for f in sorted(os.listdir(datapath)) if f.endswith('res.js')]
        old = map(simplejson.loads, old)
        new = map(simplejson.loads, new)
        res = map(simplejson.loads, res)
        for old,new,res in zip(old, new, res):
            self.assertEqual(comp(new, old), res)
