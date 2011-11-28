#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Communicates with Cassandra.
"""

import logging
import zlib
import ujson as json
from copy import copy
from telephus.pool import CassandraClusterPool
from telephus.cassandra.c08.ttypes import NotFoundException
from twisted.internet import threads

from twisted.internet.defer import inlineCallbacks, returnValue
from hiispider.components.base import Component, shared
from hiispider.components.logger import Logger
import binascii

LOGGER = logging.getLogger(__name__)


def compress(obj):
    """Dump obj to JSON, then compress with gzip."""
    return zlib.compress(json.dumps(obj))


def decompress(s):
    """Decompress with gzip, then load obj from JSON string"""
    return json.loads(zlib.decompress(s))


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
        self.cf_delta = config["cassandra_cf_delta"]
        self.cf_delta_user = config["cassandra_cf_delta_user"]
        self.cf_identity = config["cassandra_cf_identity"]
        self.cf_connections = config["cassandra_cf_connections"]
        self.cf_recommendations = config["cassandra_cf_recommendations"]
        self.cf_reverse_recommendations = config["cassandra_cf_reverse_recommendations"]

    def initialize(self):
        LOGGER.info('Initializing %s' % self.__class__.__name__)
        self.client = CassandraClusterPool(
            self.servers,
            keyspace=self.keyspace,
            pool_size=self.pool_size)
        self.client.startService()
        LOGGER.info('%s initialized, connected to: %s.' % (self.__class__.__name__, self.servers))

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
    def get(self, *args, **kwargs):
        return self.client.get(*args, **kwargs)

    @shared
    def get_key_range(self, *args, **kwargs):
        return self.client.get_key_range(*args, **kwargs)

    @shared
    def get_slice(self, *args, **kwargs):
        return self.client.get_slice(*args, **kwargs)

    @shared
    @inlineCallbacks
    def get_delta(self, delta_id):
        """Get data from cassandra by user delta_id."""
        try:
            columns = yield self.client.get_slice(
                key=binascii.unhexlify(delta_id),
                column_family=self.cf_delta)
        except NotFoundException:
            LOGGER.error("%s not found." % delta_id)
            return
        results = dict([(x.column.name, x.column.value) for x in columns])
        results["data"] = decompress(results["data"])
        if "old_data" in results:
            results["old_data"] = decompress(results["old_data"])
        if "new_data" in results:
            results["new_data"] = decompress(results["new_data"])
        returnValue(results)     

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
    def getData(self, job, consistency=1):
        try:
            data = yield self.client.get(
                key=str(job.user_account["user_id"]),
                column_family=self.cf_content,
                column=job.uuid,
                consistency=consistency)
        except NotFoundException:
            return
        obj = yield threads.deferToThread(decompress, data.column.value)
        returnValue(obj)

    @shared
    @inlineCallbacks
    def setData(self, user_id, data, uuid):
        s = yield threads.deferToThread(compress, data)
        result = yield self.client.insert(
            str(user_id),
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
