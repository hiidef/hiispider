#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Encapsulates the pagegetter. Responsible for cached HTTP calls."""

import logging
from copy import copy

from hiispider.components.base import Component, shared, broadcasted
from hiispider.components import Redis, Cassandra, Logger
from hiispider.pagegetter import PageGetter as pg
from twisted.internet import task
from pprint import pformat
from twisted.internet.defer import inlineCallbacks, returnValue
from .base import MetaComponent
from ..sleep import Sleep
LOGGER = logging.getLogger(__name__)


class PageGetter(MetaComponent):
    """Implements the getPage RPC call."""

    requires = [Redis, Cassandra]
    pg = None
    statusloop = None

    def __init__(self, server, config, server_mode, **kwargs):
        super(PageGetter, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)

    def initialize(self):
        LOGGER.info('Initializing %s' % self.__class__.__name__) 
        self.pg = pg(
            self.server.cassandra,
            redis_client=self.server.redis,
            rq=self.server.rq)
        self.statusloop = task.LoopingCall(self.status_check)
        self.statusloop.start(60)
        LOGGER.info('%s initialized.' % self.__class__.__name__)

    def status_check(self):
        pass
    
    @inlineCallbacks
    def shutdown(self):
        if self.statusloop:
            self.statusloop.stop()
        while self.server.rq.getActive() > 0:
            LOGGER.info("%s waiting on %s before shutdown." % (
                self.__class__.__name__, 
                pformat(self.server.rq.active_reqs.keys())))
            yield Sleep(1)
        returnValue(None)

    @shared
    def getPage(self, *args, **kwargs):
        return self.pg.getPage(*args, **kwargs)

    @broadcasted
    def setHostMaxRequestsPerSecond(self, *args, **kwargs):
        return self.server.rq.setHostMaxRequestsPerSecond(*args, **kwargs)

    @broadcasted
    def setHostMaxSimultaneousRequests(self, *args, **kwargs):
        return self.server.rq.setHostMaxSimultaneousRequests(*args, **kwargs)

    @shared
    def disableNegativeCache(self):
        self.pg.disable_negative_cache = True 