from ..components.base import Component, shared, broadcasted
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy
from ..pagegetter import PageGetter as pg
from ..requestqueuer import RequestQueuer as rq
import logging


LOGGER = logging.getLogger(__name__)


class PageGetter(Component):

    def __init__(self, server, config, address=None, **kwargs):
        super(PageGetter, self).__init__(server, address=address)
        config = copy(config)
        config.update(kwargs)
        if self.server_mode:
            self.rq = rq(
                max_simultaneous_requests=config["max_simultaneous_requests"],
                max_requests_per_host_per_second=config["max_requests_per_host_per_second"],
                max_simultaneous_requests_per_host=config["max_simultaneous_requests_per_host"])
            self.rq.setHostMaxRequestsPerSecond("127.0.0.1", 0)
            self.rq.setHostMaxSimultaneousRequests("127.0.0.1", 0)

    def initialize(self):
        if self.server_mode:
            LOGGER.info('Initializing %s' % self.__class__.__name__) 
            self.pg = pg(
                self.server.cassandra.client,
                redis_client=self.server.redis,
                rq=self.rq)
            self.initialized = True
            LOGGER.info('%s initialized.' % self.__class__.__name__)

    @shared
    def getPage(self, *args, **kwargs):
        return self.pg.getPage(*args, **kwargs)
    
    @broadcasted
    def setHostMaxRequestsPerSecond(self, *args, **kwargs):
        return self.rq.setHostMaxRequestsPerSecond(*args, **kwargs)

    @broadcasted
    def setHostMaxSimultaneousRequests(self, *args, **kwargs):
        return self.rq.setHostMaxSimultaneousRequests(*args, **kwargs)
