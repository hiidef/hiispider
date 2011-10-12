from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy
from telephus.pool import CassandraClusterPool
import logging


LOGGER = logging.getLogger(__name__)


class Cassandra(Component):

    def __init__(self, server, config, address=None, **kwargs):
        super(Cassandra, self).__init__(server, address=address)
        config = copy(config)
        config.update(kwargs)
        self.servers = config["cassandra_servers"]
        self.keyspace = config["cassandra_keyspace"]
        self.pool_size = len(config["cassandra_servers"]) * 2  

    def initialize(self):
        LOGGER.info('Initializing %s' % self.__class__.__name__)        
        self.client = CassandraClusterPool(
            self.servers,
            keyspace=self.keyspace,
            pool_size=self.pool_size)
        self.client.startService()
        LOGGER.info('%s initialized.' % self.__class__.__name__)        

    def shutdown(self):
        LOGGER.info("Stopping %s" % self.__class__.__name__) 
        self.client.stopService()   
        LOGGER.info("%s stopped." % self.__class__.__name__)
            
    @shared
    def batch_insert(self, *args, **kwargs):
        return self.client.batch_insert(*args, **kwargs)
            
    @shared
    def insert(self, *args, **kwargs):
        return self.client.insert(*args, **kwargs)

    @shared
    def get(self, *args, **kwargs):
        return self.client.get(*args, **kwargs)

    @shared
    def remove(self, *args, **kwargs):
        return self.client.remove(*args, **kwargs)

    @shared
    def get_indexed_slices(self, *args, **kwargs):
        return self.client.get_indexed_slices(*args, **kwargs)

    @shared
    def get_range_slices(self, *args, **kwargs):
        return self.client.get_range_slices(*args, **kwargs)

    @shared
    def get_slice(self, *args, **kwargs):
        return self.client.get_slice(*args, **kwargs)