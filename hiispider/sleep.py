#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""An asynchronous implementation of time.sleep()"""


from twisted.internet.defer import Deferred
from twisted.internet import reactor

# FIXME: make this a function rather than a class
class Sleep(Deferred):
    def __init__(self, timeout):
        Deferred.__init__(self)
        reactor.callLater(timeout, self.callback, None)

