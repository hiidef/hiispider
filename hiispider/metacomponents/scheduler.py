#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Schedules."""

import time
import random
from heapq import heappush, heappop
from twisted.internet import task
from ..components import *
from ..metacomponents import *
import logging
from .base import MetaComponent

logger = logging.getLogger(__name__)


class Scheduler(MetaComponent):

    heap = []
    allow_clients = False
    enqueueloop = None
    redistribute = True

    def __init__(self, server, config, server_mode, queue, **kwargs):
        self.queue = queue
        super(Scheduler, self).__init__(server, server_mode)

    def start(self):
        self.enqueueloop = task.LoopingCall(self.enqueue)
        self.enqueueloop.start(1)

    def shutdown(self):
        if self.enqueueloop:
            self.enqueueloop.stop()

    def is_valid(self, item):
        return True

    def add(self, item, interval):
        if interval == 0:
            return
        enqueue_time = int(time.time() + random.randint(0, interval))
        heappush(self.heap, (enqueue_time, (item, interval)))

    def enqueue(self):
        # Enqueue jobs
        now = int(time.time())
        # Compare the heap min timestamp with now().
        # If it's time for the item to be queued, pop it, update the
        # timestamp and add it back to the heap for the next go round.
        if self.queue.queue_size < 100000:
            i = 0
            logger.debug(len(self.heap))
            while self.heap and self.heap[0][0] < now:
                enqueue_time, (item, interval) = heappop(self.heap) # x is (enqueue_time, (item, interval))
                i += 1
                if self.is_valid(item):
                    # support for complex types, just set 'bytes'
                    if hasattr(item, 'bytes'):
                        self.queue.publish(item.bytes)
                    else:
                        self.queue.publish(item)
                    heappush(self.heap, (now + interval, (item, interval)))
                    if hasattr(item, 'type'):
                        self.stats.increment('scheduler.job.%s' % (item.type.replace('/', '.')), 0.1)
            if i:
                logger.debug("Added %s items to the queue." % i)
        elif self.heap and self.redistribute:
            enqueue_time, (item, interval) = heappop(self.heap)
            distribution = random.randint(-1 * interval / 2, interval / 2)
            heappush(self.heap, (now + interval + distribution, (item, interval)))
            #logger.critical('%s is at or beyond max limit (%d/100000)'
            #    % (self.__class__.__name__, self.queue.queue_size))


