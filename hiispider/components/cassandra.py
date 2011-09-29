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
        if self.server_mode:
            LOGGER.info('Initializing %s' % self.__class__.__name__)        
            self.cassandra_cf_content = config["cassandra_cf_content"]
            self.client = CassandraClusterPool(
                config["cassandra_servers"],
                keyspace=config["cassandra_keyspace"],
                pool_size=len(config["cassandra_servers"]) * 2)
            self.client.startService()
            self.initialized = True
            LOGGER.info('%s initialized.' % self.__class__.__name__)        

    def shutdown(self):
        if self.server_mode:
            self.client.stopService()
            
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