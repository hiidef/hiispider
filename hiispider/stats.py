#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Mock stats support."""

# mock logd stats collection so that stats logged by this library do not cause
# any problems if a configured stats object is not provided

class Noop(object):
    def __getattr__(self, attr):
        return self.noop

    def noop(self, *args, **kwargs):
        return

class StatsNoop(Noop):
    def __init__(self):
        super(StatsNoop, self).__init__()
        self.timer = Noop()

    def timed(self, *args, **kwargs):
        def decorator(f):
            return f
        return decorator

# the server should set this to the proper stats object if one is configured
stats = StatsNoop()

