from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy
from telephus.pool import CassandraClusterPool
import logging
import zlib
import simplejson
from telephus.cassandra.c08.ttypes import NotFoundException

LOGGER = logging.getLogger(__name__)


class Cassandra(Component):

    def __init__(self, server, config, server_mode, **kwargs):
        super(Cassandra, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        self.servers = config["cassandra_servers"]
        self.keyspace = config["cassandra_keyspace"]
        self.pool_size = len(config["cassandra_servers"]) * 2  
        self.cf_content = config["cassandra_cf_content"]

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
    def remove(self, *args, **kwargs):
        return self.client.remove(*args, **kwargs)
    
    @shared
    @inlineCallbacks
    def getDataByIDAndUUID(self, user_id, uuid):
        """Get data from cassandra by user id and uuid."""
        try:
            data = yield self.client.get(
                key=str(user_id),
                column_family=self.cf_content,
                column=uuid)
            returnValue(simplejson.loads(zlib.decompress(data.column.value)))
        except NotFoundException:
            return

    @shared
    @inlineCallbacks
    def getData(self, job):
        try:
            data = yield self.client.get(
                key=str(job.user_account["user_id"]),
                column_family=self.cf_content,
                column=job.uuid)
            returnValue(simplejson.loads(zlib.decompress(data.column.value)))
        except NotFoundException:
            return

    @shared
    def setData(self, data, job):
        return self.client.insert(
            str(job.user_account["user_id"]),
            self.cf_content,
            zlib.compress(simplejson.dumps(data)),
            column=job.uuid)

#    @shared
#    def get(self, *args, **kwargs):
#        return self.client.get(*args, **kwargs)
#
#    @shared
#    def get_indexed_slices(self, *args, **kwargs):
#        return self.client.get_indexed_slices(*args, **kwargs)
#
#    @shared
#    def get_range_slices(self, *args, **kwargs):
#        return self.client.get_range_slices(*args, **kwargs)
#
#    @shared
#    def get_slice(self, *args, **kwargs):
#        return self.client.get_slice(*args, **kwargs)#