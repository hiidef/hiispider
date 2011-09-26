#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A spider mixin that saves job history in redis."""

import time
import re

from collections import defaultdict
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

    def saveJobHistory(self, job, success):
        if not self.jobhistory_enabled or not job.uuid:
            return
        key = "job:%s:%s" % (job.uuid, 'good' if success else 'bad')
        self.jobhistory_client._send('LPUSH', key, time.time())
        self.jobhistory_client._send('LTRIM', key, 0, 9)