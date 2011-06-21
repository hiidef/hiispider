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

    def test_basics(self, comp=None):
        """Basic list comparisson tests."""
        comp = comp or delta._compare_lists
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

    def test_basics(self, comp=None):
        """Basic dict comparisson tests."""
        comp = comp or delta._compare_dicts
        # test the identify comparisson against an empty dict
        for i in range(10):
            keys = [''.join(random.sample('abcdefghijklmnopqrstuvwxyz', 3)) for x in range(100)]
            values = random.sample(xrange(10000), 100)
            d = dict(zip(keys, values))
            # compare_dicts returns a list of maps of new-keys to new values
            self.assertEqual(srt(comp(d, {})), srt([{k:v} for k,v in d.iteritems()]))

    def test_corpus(self, comp=None):
        """Use tests/deltas/ test case data."""
        full = lambda x: os.path.join(datapath, x)
        comp = comp or delta._compare_dicts
        old = [read(full(f)).decode('base64') for f in sorted(os.listdir(datapath)) if f.endswith('old.js')]
        new = [read(full(f)).decode('base64') for f in sorted(os.listdir(datapath)) if f.endswith('new.js')]
        res = [read(full(f)).decode('base64') for f in sorted(os.listdir(datapath)) if f.endswith('res.js')]
        old = map(simplejson.loads, old)
        new = map(simplejson.loads, new)
        res = map(simplejson.loads, res)
        for old,new,res in zip(old, new, res):
            self.assertEqual(comp(new, old), res)

class CompareAutogenerate(CompareDictsTest, CompareListsTest):
    """Test delta.autogenerate."""

    def test_basics(self):
        """Test list & dict basics with autogenerate."""
        CompareListsTest.test_basics(self, comp=delta.autogenerate)
        CompareDictsTest.test_basics(self, comp=delta.autogenerate)

    def test_corpus(self):
        """Test corpus with autogenerate."""
        CompareDictsTest.test_corpus(self, comp=delta.autogenerate)

    def test_exceptional_cases(self):
        """Test exceptional cases for autogenerate."""
        # comparing strings throws a type error
        args = ('foo', 'bar')
        self.assertRaises(TypeError, delta.autogenerate, args)
        # UNLESS(?) they're the same
        args = ('foo', 'foo')
        self.assertEqual(delta.autogenerate(*args), [])
        # raise type error if classes aren't the same
        args = (['foo'], {'bar': 1})
        self.assertRaises(TypeError, delta.autogenerate, args)
        # raises an error if it doesn't know about what classes they are
        class Flub(object):
            def __init__(self, val):
                self.val = val
        self.assertRaises(TypeError, delta.autogenerate, (Flub('hi'), Flub('bye')))
        # but it doesn't if cmp works on the object and they're the same...
        class Flub2(Flub):
            def __cmp__(self, other):
                return cmp(self.val, other.val)
        self.assertEqual(delta.autogenerate(Flub2('hi'), Flub2('hi')), [])

