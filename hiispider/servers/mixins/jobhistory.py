#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A spider mixin that saves job history in redis."""

import time
import re

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, waitForDeferred
from txredisapi import RedisConnectionPool

safere = re.compile('[^a-zA-Z0-9_]')
def keysafe(string):
    return safere.sub('.', string)

class JobHistoryMixin(object):
    jobhistory_enabled = False

    @inlineCallbacks
    def setupJobHistory(self, config):
        conf = config.get('jobhistory', {})
        if not conf or not conf.get('enabled', False):
            returnValue(None)
        host, port = conf['host'].split(':')
        self.jobhistory_client = yield RedisConnectionPool(host, int(port))
        self.jobhistory_enabled = True
        returnValue(None)

    @inlineCallbacks
    def saveJobHistory(self, job, success):
        if not self.jobhistory_enabled:
            returnValue(None)
        if not job.uuid:
            returnValue(None)
        t = time.time()
        key = "job:%s:%s" % (job.uuid, 'good' if success else 'bad')
        yield self.jobhistory_client.lpush(key, t)
        yield self.jobhistory_client.ltrim(key, 0, 9)
        returnValue(None)

