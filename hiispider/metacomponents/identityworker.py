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
from traceback import format_exc, format_tb
from twisted.internet import task, reactor
from collections import defaultdict
from twisted.internet.defer import inlineCallbacks, maybeDeferred
from pprint import pformat


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
        self.plugin_mapping = config["plugin_mapping"]

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
        total_time = sum(self.timer.values())
        total_count = sum(self.timer_count.values())
        mean_time = total_time / len(self.timer)
        mean_time_per_call = sum([self.timer[x]/self.timer_count[x] for x in self.timer]) / len(self.timer)
        total_times = [(x[0], x[1] / mean_time) for x in self.timer.items()]
        average_times = [(x, self.timer[x]/(self.timer_count[x] * mean_time_per_call)) for x in self.timer]
        LOGGER.info("Total times:\n %s" % pformat(sorted(total_times, key=lambda x:x[1])))
        LOGGER.info("Average times:\n %s" % pformat(sorted(average_times, key=lambda x:x[1])))
        LOGGER.info("Wait time by component:\n%s" % "\n".join(["%s:%s" % (x.__class__.__name__, x.wait_time) for x in self.server.components]))

    
    def get_service_connections(self, user):    
        service = self.plugin_mapping.get(
            user["_account_type"], 
            user["_account_type"])
        function_key = "%s/_getconnections" % service
        f = self.server.functions[function_key]
        d = maybeDeferred(f["function"], **user)
        d.addCallback(self._get_service_connections_callback, service, user)
        d.addErrback(self._get_service_connections_errback)
        return d

    @inlineCallbacks
    def _get_service_connections_callback(self, ids, service, user):
        ids = set(ids)
        # Currently stored connections
        current = yield self.server.cassandra.getServiceConnections(
            service, 
            user["user_id"])
        # Remove Currently stored connections no longer in the service
        yield self.server.cassandra.removeConnections(
            service, 
            user["user_id"], 
            dict([x for x in current.items() if x[0] in set(current) - ids]))
        # Add connections in the service not currently stored
        yield self.server.cassandra.addConnections(
            service, 
            user["user_id"],
            ids - set(current))

    def _get_service_connections_errback(self, error):
        try:
            error.raiseException()
        except NotImplementedError:
            return
        except Exception, e:
            tb = '\n'.join(format_tb(error.getTracebackObject()))
            LOGGER.error("Error getting service connections: %s\n%s" % (
                tb,
                format_exc()))

    def get_service_identity(self, user):
        service = self.plugin_mapping.get(
            user["_account_type"], 
            user["_account_type"])
        function_key = "%s/_getidentity" % service
        f = self.server.functions[function_key]
        d = maybeDeferred(f["function"], **user)
        d.addCallback(self._get_service_identity_callback, service, user)
        d.addErrback(self._get_service_identity_errback)
        return d

    @inlineCallbacks
    def _get_service_identity_callback(self, service_id, service, user):
        yield self.server.cassandra.setServiceIdentity(
            service, 
            user["user_id"], 
            service_id) 

    def _get_service_identity_errback(self, error):
        try:
            error.raiseException()
        except NotImplementedError:
            return
        except Exception, e:
            tb = '\n'.join(format_tb(error.getTracebackObject()))
            LOGGER.error("Error getting identity connections: %s\n%s" % (
                tb,
                format_exc()))

    @inlineCallbacks
    def getUsers(self):
        self.getting_users = True
        try:
            users = yield self.server.identitygetter.getUsers()
            self.user_queue.extend(users)
        except Exception:
            LOGGER.error(format_exc())
        self.getting_users = False  

    @inlineCallbacks
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
        service = self.plugin_mapping.get(
            user["_account_type"], 
            user["_account_type"])
        # Identity
        self.time_start(r)
        try:
            yield self.get_service_identity(user)
        except:
            LOGGER.error(format_exc())
        self.time_end(r, "%s identity" % service)
        # Connections
        self.time_start(r)
        try:
            yield self.get_service_connections(user)
        except:
            LOGGER.error(format_exc())
        self.time_end(r, "%s connections" % service)
        # Connections
        reactor.callLater(0, self.work)
        