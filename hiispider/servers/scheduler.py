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
from twisted.internet.defer import inlineCallbacks, returnValue
from txamqp.content import Content
from .base import BaseServer
from .mixins import MySQLMixin, JobQueueMixin, IdentityQueueMixin

logger = logging.getLogger(__name__)

from twisted.web.resource import Resource


class SchedulerServer(BaseServer, MySQLMixin, JobQueueMixin, IdentityQueueMixin):

    jobs_heap = []
    identity_heap = []
    # TODO: make this a set()
    removed_job_uuids = []
    removed_identity_ids = []
    enqueueJobCallLater = None
    enqueueloop = None

    def __init__(self, config, port=None):
        super(SchedulerServer, self).__init__(config)
        self.setupJobQueue(config)
        self.setupIdentityQueue(config)
        self.setupMySQL(config)
        # HTTP interface
        resource = Resource()
        self.function_resource = Resource()
        resource.putChild("function", self.function_resource)
        if port is None:
            port = config["scheduler_server_port"]
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Logging, etc
        self.expose(self.removeFromJobsHeap)
        self.expose(self.addToJobsHeap)
        self.expose(self.addToIdentityHeap)
        self.expose(self.removeFromIdentityHeap)
        self.expose(self.enqueueJobUUID)

    def start(self):
        start_deferred = super(SchedulerServer, self).start()
        start_deferred.addCallback(self._schedulerStart)
        return start_deferred

    @inlineCallbacks
    def _schedulerStart(self, started=None):
        yield self.startJobQueue()
        yield self.startIdentityQueue()
        yield self._loadFromMySQL()
        self.enqueueloop = task.LoopingCall(self.enqueue)
        self.enqueueloop.start(1)

    @inlineCallbacks
    def _loadFromMySQL(self):
        # Get jobs
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
                self.addToJobsHeap(row["uuid"], row["type"])
        data = []
        start = 0
        # Get user_ids
        while len(data) >= 100000 or start == 0:
            sql = """SELECT DISTINCT user_id 
                     FROM content_account
                     ORDER BY user_id LIMIT %s, 100000
                  """ % start
            start += 100000
            data = yield self.mysql.runQuery(sql)
            for row in data:
                self.addToIdentityHeap(int(row["user_id"]))

    @inlineCallbacks
    def shutdown(self):
        self.enqueueloop.stop()
        try:
            self.enqueueJobCallLater.cancel()
        except:
            pass
        yield self.stopJobQueue()
        yield self.stopIdentityQueue()
        yield super(SchedulerServer, self).shutdown()

    def enqueue(self):
        # Enqueue jobs
        now = int(time.time())
        logger.debug("%s:%s" % (self.jobs_heap[0][0], now))
        # Compare the heap min timestamp with now().
        # If it's time for the item to be queued, pop it, update the
        # timestamp and add it back to the heap for the next go round.
        queued_items = 0
        self.stats.change_by('unscheduled.size', len(self.removed_job_uuids))
        if self.amqp_jobs_queue_size < 100000:
            logger.debug("Jobs: %s:%s" % (self.jobs_heap[0][0], now))
            while self.jobs_heap[0][0] < now and queued_items < 1000:
                job = heappop(self.jobs_heap)
                uuid = UUID(bytes=job[1][0])
                if not uuid.hex in self.removed_job_uuids:
                    queued_items += 1
                    self.jobs_chan.basic_publish(
                        exchange=self.amqp_exchange,
                        content=Content(job[1][0]))
                    self.stats.increment('chan.enqueue.success')
                    heappush(self.jobs_heap, (now + job[1][1], job[1]))
                else:
                    self.removed_job_uuids.remove(uuid.hex)
        else:
            logger.critical('AMQP jobs queue is at or beyond max limit (%d/100000)'
                % self.amqp_jobs_queue_size)
        # Enqueue identity
        queued_items = 0
        if self.amqp_identity_queue_size < 100000:
            logger.debug("Identity: %s:%s" % (self.identity_heap[0][0], now))
            while self.identity_heap[0][0] < now and queued_items < 1000:
                job = heappop(self.identity_heap)
                if not job[1][0] in self.removed_identity_ids:
                    queued_items += 1
                    self.identity_chan.basic_publish(
                        exchange=self.amqp_exchange,
                        content=Content(str(job[1][0])))
                    heappush(self.identity_heap, (now + job[1][1], job[1]))
                else:
                    self.removed_identity_ids.remove(user_id)
        else:
            logger.critical('AMQP jobs queue is at or beyond max limit (%d/100000)'
                % self.amqp_identity_queue_size)

    def enqueueJobUUID(self, uuid):
        logger.debug('enqueueJobUUID: uuid=%s' % uuid)
        self.jobs_chan.basic_publish(
            exchange=self.amqp_exchange,
            content=Content(UUID(uuid).bytes))
        return uuid
    
    def addToIdentityHeap(self, user_id):
        if not user_id in self.removed_identity_ids:
            interval = 24 * 60 * 60 * 7
            enqueue_time = int(time.time() + random.randint(0, interval))
            logger.debug('Adding %s to heap with time %s and interval of %s'
                % (user_id, enqueue_time, interval))
            heappush(self.identity_heap, (enqueue_time, (user_id, interval)))
        else:
            logger.info('Unscheduling user %s' % user_id)
            self.removed_identity_ids.remove(user_id)

    def removeFromIdentityHeap(self, user_id):
        logger.info('Removing %s from identity heap' % user_id)
        self.removed_identity_ids.append(user_id)

    def addToJobsHeap(self, uuid, type):
        # lookup if type is in the service_mapping, if it is
        # then rewrite type to the proper resource
        if not uuid in self.removed_job_uuids:
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
                logger.debug('Adding %s to jobs heap with time %s and interval of %s'
                    % (uuid, enqueue_time, interval))
                heappush(self.jobs_heap, (enqueue_time, (uuid_bytes, interval)))
        else:
            logger.info('Unscheduling job %s' % uuid)
            self.removed_job_uuids.remove(uuid)

    def removeFromJobsHeap(self, uuid):
        logger.info('Removing %s from heap' % uuid)
        self.removed_job_uuids.append(uuid)



