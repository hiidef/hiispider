#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Communicates with Redis
"""

import logging
from copy import copy
from twisted.internet.defer import inlineCallbacks
from txredisapi import RedisShardingConnection
from .base import Component, shared
from .logger import Logger


LOGGER = logging.getLogger(__name__)


class Redis(Component):

    """
    Implements basic Redis functions as RPC calls.
    """

    client = None

    def __init__(self, server, config, server_mode, **kwargs):
        super(Redis, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        self.hosts = config["redis_hosts"]

    @inlineCallbacks
    def initialize(self):
        LOGGER.info("Initializing %s" % self.__class__.__name__)
        try:
            self.client = yield RedisShardingConnection(self.hosts)
        except Exception, e:
            LOGGER.error("Could not connect to Redis: %s" % e)
            self.server.shutdown()
            raise Exception("Could not connect to Redis.")
        LOGGER.info("%s initialized." % self.__class__.__name__)

    @inlineCallbacks
    def shutdown(self):
        LOGGER.info("Disconnecting %s" % self.__class__.__name__)
        yield self.client.disconnect()
        LOGGER.info("%s disconnected." % self.__class__.__name__)

    @shared
    def get(self, *args, **kwargs):
        return self.client.get(*args, **kwargs)

    @shared
    def mget(self, *args, **kwargs):
        return self.client.mget(*args, **kwargs)

    @shared
    def set(self, *args, **kwargs):
        return self.client.set(*args, **kwargs)

    @shared
    def mset(self, *args, **kwargs):
        return self.client.mset(*args, **kwargs)

    @shared
    def expire(self, *args, **kwargs):
        return self.client.expire(*args, **kwargs)

    @shared
    def delete(self, *args, **kwargs):
        return self.client.delete(*args, **kwargs)



