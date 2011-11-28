#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Updates Identities."""


import logging
import time
from random import random
from .base import MetaComponent
from copy import copy
from hiispider.components import *
from hiispider.metacomponents import PageGetter, IdentityGetter
from traceback import format_exc
from twisted.internet import task, reactor
from collections import defaultdict
from twisted.internet.defer import inlineCallbacks

LOGGER = logging.getLogger(__name__)


class IdentityWorker(MetaComponent):

    statusloop = None
    simultaneous_jobs = 100
    user_queue = []
    requires = [Logger, MySQL, Cassandra, PageGetter, IdentityGetter]
    getting_users = False
    timer = defaultdict(lambda:0)
    timer_count = defaultdict(lambda:0)
    timer_starts = {}

    def __init__(self, server, config, server_mode, **kwargs):
        super(IdentityWorker, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        self.mysql = self.server.mysql

    def time_start(self, task_id):
        self.timer_starts[task_id] = time.time()

    def time_end(self, task_id, task_name, add=0):
        self.timer_count[task_name] += 1
        self.timer[task_name] += add + time.time()
        # FIXME: why isn't time_starts[task_id] available
        if task_id in self.timer_starts:
            self.timer[task_name] -= self.timer_starts[task_id]
            del self.timer_starts[task_id]
    
    def start(self):
        self.statusloop = task.LoopingCall(self.status)
        self.statusloop.start(60, False)
        for i in range(0, self.simultaneous_jobs):
            self.work()

    def shutdown(self):
        if self.statusloop:
            self.statusloop.stop()

    def status(self):
        pass

    @inlineCallbacks
    def getUsers(self):
        self.getting_users = True
        try:
            users = yield self.server.identitygetter.getUsers()
            self.user_queue.extend(users)
        except Exception:
            LOGGER.error(format_exc())
        self.getting_users = False


    def work(self):
        if not self.running:
            return
        if len(self.user_queue) < 20 and not self.getting_users:
            self.getUsers()
        r = random()
        self.time_start(r)
        if self.user_queue:
            user = self.user_queue.pop(0)
            self.time_end(r, "get_users")
        else:
            self.time_end(r, "get_users", add=.1)
            reactor.callLater(.1, self.work)
            return
        LOGGER.debug(user)
        reactor.callLater(0, self.work)
        