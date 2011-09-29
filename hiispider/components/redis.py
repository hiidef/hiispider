from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy
from txredisapi import RedisShardingConnection
import logging


LOGGER = logging.getLogger(__name__)


class Redis(Component):

    client = None

    def __init__(self, server, config, address=None, **kwargs):
        super(Redis, self).__init__(server, address=address)
        config = copy(config)
        config.update(kwargs)
        self.hosts = config["redis_hosts"]
    
    @inlineCallbacks
    def initialize(self):
        LOGGER.info("Initializing %s" % self.__class__.__name__)
        if self.server_mode:
            try:
                self.client = yield RedisShardingConnection(self.hosts)
            except Exception, e:
                LOGGER.error("Could not connect to Redis: %s" % e)
                self.server.shutdown()
                raise Exception("Could not connect to Redis.")
            self.initialized = True
    
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
