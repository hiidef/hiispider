#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A spider mixin that saves job history in redis."""

import time
import re

from collections import defaultdict
from twisted.internet import reactor, protocol
from twisted.internet.defer import inlineCallbacks, returnValue, waitForDeferred
from txredis.protocol import Redis
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
            logger.info("Connecting to JobHistory Redis.")
            clientCreator = protocol.ClientCreator(reactor, Redis)
            self.jobhistory_client = yield clientCreator.connectTCP(
                self.jobhistory_host, 
                int(self.jobhistory_port))
            logger.info("Connected to JobHistory Redis.")
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