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

LOGGER = logging.getLogger(__name__)


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

            # LOGGER.debug("%s : %s" % (self.__class__.__name__, now))
            i = 0
            
            LOGGER.debug(len(self.heap))
            while self.heap and self.heap[0][0] < now:
                x = heappop(self.heap) # x is (enqueue_time, (item, interval))
                i += 1
                if self.is_valid(x[1][0]): # Check for valid item
                    self.queue.publish(x[1][0]) # Publish if valid
                    # push item/interval back on heap
                    heappush(self.heap, (now + x[1][1], x[1]))
            if i:
                LOGGER.debug("Added %s items to the queue." % i)
        elif self.heap and self.redistribute:
            x = heappop(self.heap)
            distribution = random.randint(-1 * x[1][1] / 2, x[1][1] / 2)
            heappush(self.heap, (now + x[1][1] + distribution, x[1]))
            LOGGER.critical('%s is at or beyond max limit (%d/100000)'
                % (self.__class__.__name__, self.queue.queue_size))


