#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Encapsulates the pagegetter. Responsible for cached HTTP calls."""

import logging
from copy import copy

from hiispider.components.base import Component, shared, broadcasted
from hiispider.components import Redis, Cassandra, Logger
from hiispider.pagegetter import PageGetter as pg


LOGGER = logging.getLogger(__name__)


class PageGetter(Component):
    """Implements the getPage RPC call."""

    requires = [Redis, Cassandra, Logger]
    pg = None

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
        LOGGER.info('%s initialized.' % self.__class__.__name__)

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
    def ping(self):
        return "PONG"
