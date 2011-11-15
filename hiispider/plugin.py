#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Hiispider plugin class and plugin utilities."""

import inspect
import types
import time
from hiispider.delta import Autogenerator

__all__ = ["aliases", "expose", "make_callable", "HiiSpiderPlugin", "delta", "Stopwatch"]


EXPOSED_FUNCTIONS = {}
CALLABLE_FUNCTIONS = {}
MEMOIZED_FUNCTIONS = {}
FUNCTION_ALIASES = {}
DELTA_FUNCTIONS = {}

class Stopwatch(object):
    """A timer that allows you to make named ticks and can print a simple
    breakdown of the time between ticks after it's stopped."""
    def __init__(self, name='Stopwatch'):
        self.name = name
        self.start = time.time()
        self.ticks = []

    def tick(self, name):
        self.ticks.append((name, time.time()))

    def stop(self):
        self.stop = time.time()

    def summary(self):
        """Return a summary of timing information."""
        total = self.stop - self.start
        s = "%s duration: %0.2f\n" % (self.name, total)
        prev = ("start", self.start)
        for tick in self.ticks:
            s += ("   %s => %s" % (prev[0], tick[0])).ljust(30) + "... %0.2fs\n" % (tick[1] - prev[1])
            prev = tick
        s += ("   %s => end" % (tick[0])).ljust(30) + "... %0.2fs" % (self.stop - tick[1])
        return s

def aliases(*args):
    def decorator(f):
        FUNCTION_ALIASES[id(f)] = args
        return f
    return decorator


def autodelta(paths=None, includes=None, ignores=None, dates=None):
    def decorator(f):
        DELTA_FUNCTIONS[id(f)] = Autogenerator(paths=paths, includes=includes, ignores=ignores, dates=dates)
        return f
    return decorator


def delta(handler):
    def decorator(f):
        DELTA_FUNCTIONS[id(f)] = handler
        return f
    return decorator


def expose(func=None, interval=0, category=None, name=None, memoize=False):
    if func is not None:
        EXPOSED_FUNCTIONS[id(func)] = {"interval": interval, "name": name, 'category': category}
        return func

    def decorator(f):
        EXPOSED_FUNCTIONS[id(f)] = {"interval": interval, "name": name, 'category': category}
        return f
    return decorator


def make_callable(func=None, interval=0, category=None, name=None, memoize=False):
    if func is not None:
        CALLABLE_FUNCTIONS[id(func)] = {"interval": interval, "name": name, 'category': category}
        return func

    def decorator(f):
        CALLABLE_FUNCTIONS[id(f)] = {"interval": interval, "name": name, 'category': category}
        return f
    return decorator


class HiiSpiderPlugin(object):

    def __init__(self, spider):
        self.spider = spider
        check_method = lambda x: isinstance(x[1], types.MethodType)
        instance_methods = filter(check_method, inspect.getmembers(self))
        for instance_method in instance_methods:
            instance_id = id(instance_method[1].__func__)
            if instance_id in DELTA_FUNCTIONS:
                self.spider.server.delta(
                    instance_method[1],
                    DELTA_FUNCTIONS[instance_id])
            if instance_id in EXPOSED_FUNCTIONS:
                self.spider.server.expose(
                    instance_method[1],
                    interval=EXPOSED_FUNCTIONS[instance_id]["interval"],
                    name=EXPOSED_FUNCTIONS[instance_id]["name"],
                    category=EXPOSED_FUNCTIONS[instance_id]["category"])
                if instance_id in FUNCTION_ALIASES:
                    for name in FUNCTION_ALIASES[instance_id]:
                        self.spider.server.expose(
                            instance_method[1],
                            interval=EXPOSED_FUNCTIONS[instance_id]["interval"],
                            name=name)
            if instance_id in CALLABLE_FUNCTIONS:
                self.spider.server.expose(
                    instance_method[1],
                    interval=CALLABLE_FUNCTIONS[instance_id]["interval"],
                    name=CALLABLE_FUNCTIONS[instance_id]["name"],
                    category=CALLABLE_FUNCTIONS[instance_id]["category"])
                if instance_id in FUNCTION_ALIASES:
                    for name in CALLABLE_FUNCTIONS[instance_id]:
                        self.spider.server.expose(
                            instance_method[1],
                            interval=CALLABLE_FUNCTIONS[instance_id]["interval"],
                            name=name)

    def setFastCache(self, uuid, data):
        if not self.spider.server.jobgetter.server_mode:
            return
        if self.spider.server.config.get('enable_fast_cache', True):
            return self.spider.server.jobgetter.setFastCache(uuid, data)

    def getPage(self, *args, **kwargs):
        return self.spider.server.pagegetter.getPage(*args, **kwargs)

    def setHostMaxRequestsPerSecond(self, *args, **kwargs):
        return self.spider.server.pagegetter.setHostMaxRequestsPerSecond(*args, **kwargs)

    def setHostMaxSimultaneousRequests(self, *args, **kwargs):
        return self.spider.server.pagegetter.setHostMaxSimultaneousRequests(*args, **kwargs)
        

    @make_callable
    def _getIdentity(self, *args, **kwargs):
        raise NotImplementedError()

    @make_callable
    def _getConnections(self, *args, **kwargs):
        raise NotImplementedError()

