#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""SchedulerServer base."""

from uuid import UUID

import time
import random
import traceback
import logging

from heapq import heappush, heappop
from twisted.internet import reactor, task
from twisted.web import server
from twisted.internet.defer import inlineCallbacks
from txamqp.content import Content
from .base import BaseServer
from .mixins import MySQLMixin, AMQPMixin

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

from twisted.web.resource import Resource


class SchedulerServer(BaseServer, AMQPMixin, MySQLMixin):

    heap = []
    # TODO: make this a set()
    unscheduled_items = []
    enqueueCallLater = None
    enqueueloop = None

    def __init__(self, config, port=None):
        super(SchedulerServer, self).__init__(config)
        self.setupAMQP(config)
        self.setupMySQL(config)
        # HTTP interface
        resource = Resource()
        self.function_resource = Resource()
        resource.putChild("function", self.function_resource)
        if port is None:
            port = config["scheduler_server_port"]
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Logging, etc
        self.expose(self.remoteRemoveFromHeap)
        self.expose(self.remoteAddToHeap)
        self.expose(self.enqueueUUID)

    def start(self):
        start_deferred = super(SchedulerServer, self).start()
        start_deferred.addCallback(self._schedulerStart)
        return start_deferred

    @inlineCallbacks
    def _schedulerStart(self, started=None):
        yield self.startJobQueue()
        yield self._loadFromMySQL()
        self.enqueueloop = task.LoopingCall(self.enqueue)
        self.enqueueloop.start(1)

    @inlineCallbacks
    def _loadFromMySQL(self):
        data = []
        start = 0
        while len(data) >= 100000 or start == 0:
            sql = """SELECT uuid, type
                     FROM spider_service
                     ORDER BY id LIMIT %s, 100000
                  """ % start
            start += 100000
            data = yield self.mysql.runQuery(sql)
            for row in data:
                self.addToHeap(row["uuid"], row["type"])

    @inlineCallbacks
    def shutdown(self):
        self.enqueueloop.stop()
        try:
            self.enqueueCallLater.cancel()
        except:
            pass
        yield self.stopJobQueue()
        yield super(SchedulerServer, self).shutdown()

    def enqueue(self):
        now = int(time.time())
        # Compare the heap min timestamp with now().
        # If it's time for the item to be queued, pop it, update the
        # timestamp and add it back to the heap for the next go round.
        queued_items = 0
        if self.amqp_queue_size < 100000:
            logger.debug("%s:%s" % (self.heap[0][0], now))
            while self.heap[0][0] < now and queued_items < 1000:
                job = heappop(self.heap)
                uuid = UUID(bytes=job[1][0])
                if not uuid.hex in self.unscheduled_items:
                    queued_items += 1
                    self.chan.basic_publish(
                        exchange=self.amqp_exchange,
                        content=Content(job[1][0]))
                    heappush(self.heap, (now + job[1][1], job[1]))
                else:
                    self.unscheduled_items.remove(uuid.hex)
        else:
            logger.critical('AMQP queue is at or beyond max limit (%d/100000)'
                % self.amqp_queue_size)

    def enqueueUUID(self, uuid):
        logger.debug('enqueueUUID: uuid=%s' % uuid)
        self.chan.basic_publish(
            exchange=self.amqp_exchange,
            content=Content(UUID(uuid).bytes))
        return uuid

    def remoteAddToHeap(self, uuid=None, type=None):
        if uuid and type:
            logger.debug('remoteAddToHeap: uuid=%s, type=%s' % (uuid, type))
            self.addToHeap(uuid, type)
            return {}
        else:
            logger.error('Required parameters are uuid and type')
            return {'error': 'Required parameters are uuid and type'}

    def remoteRemoveFromHeap(self, uuid):
        logger.debug('remoteRemoveFromHeap: uuid=%s' % uuid)
        self.removeFromHeap(uuid)

    def addToHeap(self, uuid, type):
        # lookup if type is in the service_mapping, if it is
        # then rewrite type to the proper resource
        if not uuid in self.unscheduled_items:
            if self.service_mapping and type in self.service_mapping:
                logger.info('Remapping resource %s to %s'
                    % (type, self.service_mapping[type]))
                type = self.service_mapping[type]
            try:
                # Make sure the uuid is in bytes
                uuid_bytes = UUID(uuid).bytes
            except ValueError:
                logger.error('Cound not turn UUID into bytes using string: "%s" with type of "%s"'
                    % (uuid, type))
                return
            if type in self.functions and 'interval' in self.functions[type]:
                interval = int(self.functions[type]['interval'])
            else:
                logger.error('Could not find interval for type %s' % type)
                return
            # Enqueue randomly over the interval so it doesn't
            # flood the server at the interval time. only if an interval is defined
            if interval:
                enqueue_time = int(time.time() + random.randint(0, interval))
                # Add a UUID to the heap.
                logger.debug('Adding %s to heap with time %s and interval of %s'
                    % (uuid, enqueue_time, interval))
                heappush(self.heap, (enqueue_time, (uuid_bytes, interval)))
        else:
            logger.info('Unscheduling %s' % uuid)
            self.unscheduled_items.remove(uuid)

    def removeFromHeap(self, uuid):
        logger.info('Removing %s from heap' % uuid)
        self.unscheduled_items.append(uuid)

    @inlineCallbacks
    def executeReservation(self, function_name, **kwargs):
        if not isinstance(function_name, str):
            for key in self.functions:
                if self.functions[key]["function"] == function_name:
                    function_name = key
                    break
        if function_name not in self.functions:
            raise Exception("Function %s does not exist." % function_name)
        function = self.functions[function_name]
        logger.error("Calling function %s with kwargs %s" % (function_name, kwargs))
        try:
            data = yield self.executeFunction(function_name, **kwargs)
        except Exception, e:
            tb = traceback.format_exc()
            logger.error("function %s failed with args %s:\n%s" % (
                function_name, kwargs, tb))
        returnValue(data)


