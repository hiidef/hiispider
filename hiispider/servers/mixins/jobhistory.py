#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A spider mixin that saves job history in redis."""

import time
import re

from collections import defaultdict
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, waitForDeferred
from txredisapi import RedisConnectionPool
import logging


logger = logging.getLogger(__name__)


class JobHistoryMixin(object):

    jobhistory_enabled = False

    def setupJobHistory(self, config):
        conf = config.get('jobhistory', {})
        if not conf or not conf.get('enabled', False):
            return
        self.jobhistory_host, self.jobhistory_port = conf['host'].split(':')
        self.jobhistory_enabled = True

    @inlineCallbacks
    def startJobHistory(self):
        if not self.jobhistory_enabled:
            return
        try:
            self.jobhistory_client = yield RedisConnectionPool(
                self.jobhistory_host, 
                int(self.jobhistory_port))
        except Exception, e:
            logger.error("Could not connect to JobHistory Redis: %s" % e)
            self.shutdown()
            raise Exception("Could not connect to JobHistory Redis.")
 
    def saveJobHistory(self, job, success):
        if not self.jobhistory_enabled or not job.uuid:
            return
        key = "job:%s:%s" % (job.uuid, 'good' if success else 'bad')
        self.jobhistory_client._send('LPUSH', key, time.time())
        self.jobhistory_client._send('LTRIM', key, 0, 9)