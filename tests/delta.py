#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for delta functions."""

from unittest import TestCase
from hiispider import delta
from pprint import pprint
import os
import random
import time
from datetime import datetime
from hiiguid import HiiGUID

srt = lambda l: list(sorted(l))

DATAPATH = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data'))

def read(path):
    """Read a file and return its contents, cleaning up after file handles."""
    with open(path) as f:
        contents = f.read()
    return contents

def autogenerate(*args, **kwargs):
    autogenerator = delta.Autogenerator()
    return [x.data for x in autogenerator(*args, **kwargs)]

class TestAutogenerate(TestCase):
    """Test delta.autogenerate."""

    def setUp(self):
        autogenerate = delta.Autogenerator()

    def test_includes(self):
        def autogenerate_z_include(*args, **kwargs):
            autogenerator = delta.Autogenerator(includes="z")
            return [x.data for x in autogenerator(*args, **kwargs)]
        # Neither value has the included key.
        a = [{"x":1, "y":1}]
        b = [{"x":1, "y":1}, {"x":3, "y":2}]
        self.assertEqual(autogenerate_z_include(a, b), [])
        # Neither value has the included key, second value is empty.
        a = [{"x":3, "y":2}]
        b = []
        self.assertEqual(autogenerate_z_include(a, b), [])
        # 'a' has the included key.
        a = [{"x":1, "y":1}, {"x":3, "y":2, "z":1}]
        b = [{"x":1, "y":1}]
        self.assertEqual(autogenerate_z_include(a, b), [{"x":3, "y":2, "z":1}])
        # 'a' and 'b' have the included key, but it does not change.
        a = [{"x":1, "y":1}, {"x":3, "y":2, "z":1}]
        b = [{"x":1, "y":1}, {"x":3, "y":2, "z":1}, {"x":4, "y":9, "z":1}]
        self.assertEqual(autogenerate_z_include(a, b), [])
        # 'a' and 'b' have the included key, but it does not change.
        a = [{"x":1, "y":1, "z":2}, {"x":3, "y":2, "z":1}]
        b = [{"x":5, "y":5, "z":2}, {"x":9, "y":4, "z":1}]
        self.assertEqual(autogenerate_z_include(a, b), [])
        # 'a' and 'b' have the included key, and it changes.
        a = [{"x":1, "y":1}, {"x":3, "y":2, "z":1}]
        b = [{"x":1, "y":1}, {"x":3, "y":2, "z":2}, {"x":4, "y":9, "z":3}]        
        self.assertEqual(autogenerate_z_include(a, b), [{"x":3, "y":2, "z":1}])
        # 'a' and 'b' have the included key, and it changes.
        a = [{"x":1, "y":1}, {"x":3, "y":2, "z":2}, {"x":4, "y":9, "z":3}]         
        b = [{"x":1, "y":1}, {"x":3, "y":2, "z":1}]
        self.assertEqual(autogenerate_z_include(a, b), [{"x":3, "y":2, "z":2}, {"x":4, "y":9, "z":3}])
        # 'a' and 'b' have the included key, and it changes.
        a = [{"x":1, "y":1, "z":1}, {"x":3, "y":2, "z":2}, {"x":4, "y":9, "z":3}]         
        b = [{"x":4, "y":5, "z":1}]
        self.assertEqual(autogenerate_z_include(a, b), [{"x":3, "y":2, "z":2}, {"x":4, "y":9, "z":3}])
        # Nested, multiple includes
        autogenerator = delta.Autogenerator(includes=["example/y", "example/z"], paths="example")
        a = {"example":[{"x":1, "y":2, "z":3}]}
        b = {"example":[{"x":2, "y":2, "z":3}]}
        self.assertEqual(autogenerator(a, b), [])
        a = {"example":[{"x":2, "y":1, "z":3}]}
        b = {"example":[{"x":2, "y":2, "z":3}]}  
        self.assertEqual(autogenerator(a, b)[0].data, {"x":2, "y":1, "z":3})
        a = {"example":[{"x":2, "y":2, "z":3}]}
        b = {"example":[{"x":2, "y":2, "z":4}]}        
        self.assertEqual(autogenerator(a, b)[0].data, {"x":2, "y":2, "z":3})

    def test_date_parsing(self):
        def autogenerate_z_date(*args, **kwargs):
            autogenerator = delta.Autogenerator(dates="z")
            return HiiGUID(autogenerator(*args, **kwargs)[0].id).timestamp     
        now = time.time()
        b = []
        # Float
        a = [{"z":float(now)}]
        self.assertEqual(autogenerate_z_date(a, b), int(now))
        # Int
        a = [{"z":int(now)}]
        self.assertEqual(autogenerate_z_date(a, b), int(now))        
        # Str
        a = [{"z":datetime.fromtimestamp(now).isoformat()}]
        self.assertEqual(autogenerate_z_date(a, b), int(now)) 
        # With include
        autogenerator = delta.Autogenerator(dates="date", includes="z")
        a = [{"date":int(now), "z":1}]
        b = [{"date":int(now + 10), "z":1}]
        self.assertEqual(autogenerator(a, b), [])
        # With nested include
        autogenerator = delta.Autogenerator(paths='example', dates="example/date", includes="example/z")
        a = {"example":[{"date":int(now), "z":1}]}
        b = {"example":[{"date":int(now + 10), "z":1}]}
        self.assertEqual(autogenerator(a, b), []) 
        # Nested, multiple includes
        autogenerator = delta.Autogenerator(includes=["example/y", "example/z"], paths="example", dates="example/date")
        a = {"example":[{"x":1, "y":2, "z":3, "date":int(now)}]}
        b = {"example":[{"x":2, "y":2, "z":3, "date":int(now + 10)}]}
        self.assertEqual(autogenerator(a, b), [])
        a = {"example":[{"x":2, "y":1, "z":3, "date":int(now)}]}
        b = {"example":[{"x":2, "y":2, "z":3, "date":int(now + 10)}]}  
        self.assertEqual(autogenerator(a, b)[0].data, {"x":2, "y":1, "z":3, "date":int(now)})
        a = {"example":[{"x":2, "y":2, "z":3, "date":int(now)}]}
        b = {"example":[{"x":2, "y":2, "z":4, "date":int(now + 10)}]}        
        self.assertEqual(autogenerator(a, b)[0].data, {"x":2, "y":2, "z":3, "date":int(now)})


    def test_exceptional_cases(self):
        """Test exceptional cases for autogenerate."""
        # autogeneratearing strings throws a type error
        args = ('foo', 'bar')
        self.assertRaises(TypeError, autogenerate, args)
        # UNLESS(?) they're the same
        args = ('foo', 'foo')
        self.assertEqual(autogenerate(*args), [])
        # raise type error if classes aren't the same
        args = (['foo'], {'bar': 1})
        self.assertRaises(TypeError, autogenerate, args)
        # raises an error if it doesn't know about what classes they are
        class Flub(object):
            def __init__(self, val):
                self.val = val
        self.assertRaises(TypeError, autogenerate, (Flub('hi'), Flub('bye')))
        # but it doesn't if cmp works on the object and they're the same...
        class Flub2(Flub):
            def __cmp__(self, other):
                return cmp(self.val, other.val)
        self.assertEqual(autogenerate(Flub2('hi'), Flub2('hi')), [])

    def test_lists(self):
        """Basic list autogenerate comparisson tests."""
        # test the identity autogenerate comparisson against an empty list
        for i in range(10):
            rand = srt(random.sample(xrange(10000), 100))
            self.assertEqual(srt(autogenerate(rand, [])), rand)
        # test some known valued lists against eachother
        self.assertEqual(autogenerate([1,2,3,4], [2,3,4]), [1])
        # going the other way doesn't get us anything
        self.assertEqual(autogenerate([2,3,4], [1,2,3,4]), [])
        # test interpolated values
        self.assertEqual(autogenerate([1,2,3,4,5,6,7], [2,5,1,3]), [4,6,7])
        # establish what duplicates mean
        self.assertEqual(autogenerate([1,1,2,3,4,4,5], [1,2,3,4,5]), [])
        # test autogenerate comparissons on autogenerateosite types
        self.assertEqual(srt(autogenerate(
            [{'a': 1, 'b': 2}, {'c': 3}, ['foo', 'bar']],
            [{'c': 3}])),
            srt([{'a': 1, 'b': 2}, ['foo', 'bar']]),
        )
        self.assertEqual(srt(autogenerate(
            [{'a': 1, 'b': 2}, {'c': 3}, ['foo', 'bar']],
            [{'a': 1, 'b': 3}])),
            srt([{'a': 1, 'b': 2}, {'c': 3}, ['foo', 'bar']]),
        )
    
    def test_goodreads(self):
        from simplejson import loads
        old = loads(read("%s/goodreads/old.json" % DATAPATH))
        new = loads(read("%s/goodreads/old.json" % DATAPATH))
        self.assertEqual(autogenerate(old, new), [])
