#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Communicates with Cassandra.
"""

import logging
import zlib
import simplejson
from copy import copy
from telephus.pool import CassandraClusterPool
from telephus.cassandra.c08.ttypes import NotFoundException
from twisted.internet import threads
from twisted.internet.defer import inlineCallbacks, returnValue
from .base import Component, shared
from .logger import Logger


LOGGER = logging.getLogger(__name__)


def compress(obj):
    """Dump obj to JSON, then compress with gzip."""
    return zlib.compress(simplejson.dumps(obj))


def decompress(s):
    """Decompress with gzip, then load obj from JSON string"""
    return simplejson.loads(zlib.decompress(s))


class Cassandra(Component):
    """
    Implements basic Cassandra operations as well as more complex job-based
    methods.
    """

    client = None

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
        except NotFoundException:
            return
        obj = yield threads.deferToThread(decompress, data.column.value)
        returnValue(obj)

    @shared
    @inlineCallbacks
    def getData(self, job):
        try:
            data = yield self.client.get(
                key=str(job.user_account["user_id"]),
                column_family=self.cf_content,
                column=job.uuid)
        except NotFoundException:
            return
        obj = yield threads.deferToThread(decompress, data.column.value)
        returnValue(obj)

    @shared
    @inlineCallbacks
    def setData(self, data, uuid):
        s = yield threads.deferToThread(compress, data)
        result = yield self.client.insert(
            str(job.user_account["user_id"]),
            self.cf_content,
            s,
            column=uuid)
        returnValue(result)

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