#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Connects to various components to generate an internal queue of indentity objects."""

from .base import MetaComponent
from hiispider.exceptions import IdentityGetterShutdownException
from hiispider.components import MySQL, IdentityQueue, Logger, Redis, shared
from txamqp.queue import Empty
from traceback import format_exc
from twisted.internet import task
import logging
from copy import copy
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue
from collections import defaultdict
import cPickle
from zlib import decompress, compress

LOGGER = logging.getLogger(__name__)


def invert(d):
    """Invert a dictionary."""
    return dict([(v, k) for (k, v) in d.iteritems()])


class IdentityGetter(MetaComponent):
    
    dequeueloop = None
    user_id_dequeueing = False
    min_size = 10
    requires = [MySQL, IdentityQueue, Logger, Redis]
    user_id_queue = []
    uncached_queue = []
    user_queue = []
    user_requests = []

    def __init__(self, server, config, server_mode, **kwargs):
        super(IdentityGetter, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        self.min_size = config.get('identitygetter_min_size', IdentityGetter.min_size)
        self.scheduler_server = config["identity_scheduler_server"]
        self.scheduler_server_port = config["identity_scheduler_server_port"]
        self.service_args_mapping = config["service_args_mapping"]
        self.inverted_args_mapping = dict([(s[0], invert(s[1]))
            for s in self.service_args_mapping.items()])
        
    def __len__(self):
        return sum([len(self.user_id_queue), 
            len(self.uncached_queue), 
            len(self.user_queue)])
    
    def start(self):
        self.dequeueloop = task.LoopingCall(self.dequeue)
        self.dequeueloop.start(2)

    def shutdown(self):
        if self.dequeueloop:
            self.dequeueloop.stop()
        for req in self.user_requests:
            req.errback(IdentityGetterShutdownException("IdentityGetter shut down."))
        self.user_id_queue = []
    
    @shared
    def getUsers(self):
        if self.user_queue:
            users, self.user_queue = self.user_queue[0:100], self.user_queue[100:]
            return users
        else:
            d = Deferred()
            self.user_requests.append(d)
            return d
    
    def dequeue(self):
        LOGGER.debug("%s queued users." % len(self.user_queue))
        while self.user_requests:
            if self.user_queue:
                users, self.user_queue = self.user_queue[0:100], self.user_queue[100:]
                d = self.user_requests.pop(0)
                d.callback(users)
            else:
                break
        if self.user_id_queue:
            self._checkCache()
        if self.uncached_queue:
            self._lookupUsers()
        if self.user_id_dequeueing:
            return
        if len(self) > self.min_size:
            return
        self._dequeue()
    
    @inlineCallbacks
    def _dequeue(self):
        self.user_id_dequeueing = True
        self.user_id_queue = []
        while len(self) < self.min_size * 4:
            try:
                content = yield self.server.identityqueue.get(timeout=5)
            except Empty:
                continue
            except Exception, e:
                LOGGER.error(format_exc())
                break
            LOGGER.debug("Enqueing %s" % int(content))
            self.user_id_queue.append(content)
        self.user_id_dequeueing = False
        returnValue(None)
    
    @inlineCallbacks
    def _checkCache(self):  
        user_ids, self.user_id_queue = self.user_id_queue, []
        try:
            results = yield self.server.redis.mget(*user_ids)
        except Exception:
            LOGGER.error(format_exc())
            self.uncached_queue.extend(user_ids)
            return
        for row in zip(user_ids, results):
            if row[1]:
                try:
                    user = cPickle.loads(decompress(row[1]))
                    self.user_queue.append(user)
                except Exception:
                    LOGGER.error(format_exc())
                    self.uncached_queue.append(row[0])
                    continue
            else:
                self.uncached_queue.append(row[0]) 

    @inlineCallbacks
    def _lookupUsers(self):  
        accounts_by_type = defaultdict(list)
        while self.uncached_queue:
            user_ids, self.uncached_queue = self.uncached_queue[0:100], self.uncached_queue[100:]
            sql = ("SELECT type, user_id FROM content_account "
                "WHERE user_id IN (%s)" % ",".join(user_ids))
            results = yield self.server.mysql.runQuery(sql)
            for row in results:
                accounts_by_type[row["type"]].append(str(row["user_id"]))
        for account_type in accounts_by_type:
            if "custom_" in account_type:
                continue
            sql = """SELECT content_%(account_type)saccount.*, content_account.user_id
            FROM content_%(account_type)saccount
            INNER JOIN content_account
            ON content_%(account_type)saccount.account_id = content_account.id
            WHERE content_account.user_id IN (%(user_ids)s)""" % {
                "account_type": account_type,
                "user_ids": ",".join(accounts_by_type[account_type])}
            LOGGER.debug(sql)
            mapping = self.inverted_args_mapping.get(account_type, {})
            results = yield self.server.mysql.runQuery(sql)
            for row in results:
                row["_account_type"] = account_type
                for key, value in mapping.iteritems():
                    if value in row:
                        row[key] = row.pop(value)
                row["user_id"] = str(row["user_id"])
                self.server.redis.set(row["user_id"], compress(cPickle.dumps(row)))
            self.user_queue.extend(results)
            LOGGER.debug("User queue has %s items." % len(self.user_queue))
    


