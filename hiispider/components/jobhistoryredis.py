#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Jobhistory Component."""

import time

from twisted.internet.defer import inlineCallbacks, returnValue
from txredisapi import RedisConnectionPool

from hiispider.components.base import shared, Component

class JobHistoryRedis(Component):
    enabled = True

    def __init__(self, server, config, server_mode, **kwargs):
        super(JobHistoryRedis, self).__init__(server, server_mode)
        conf = config.get('jobhistory', {})
        if not conf or not conf.get('enabled', False):
            self.enabled = False
            return
        self.host, self.port = conf['host'].split(':')
        self.port = int(self.port)

    @inlineCallbacks
    def initialize(self):
        if self.enabled:
            # this uses a single redis server; do not use a sharding connection
            self.client = yield RedisConnectionPool(self.host, self.port)

    @shared
    def save(self, job, success):
        if not self.enabled or not job.uuid:
            return
        key = "job:%s:%s" % (job.uuid, 'good' if success else 'bad')
        self.client.ltrim(key, 1, 9)
        self.client.push(key, time.time())
        #self.client._send('LPUSH', key, time.time())
        #self.client._send('LTRIM', key, 0, 9)

