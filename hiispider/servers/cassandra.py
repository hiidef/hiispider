#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Cassandra using version of BaseServer."""

import os
import simplejson
import zlib
import pprint
import urllib
import traceback
import logging
import time
from uuid import uuid4
from difflib import SequenceMatcher

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient
from .base import BaseServer
from ..pagegetter import PageGetter

from txredisapi import RedisShardingConnection
from mixins.jobgetter import JobGetterMixin

PP = pprint.PrettyPrinter(indent=4)

logger = logging.getLogger(__name__)

class CassandraServer(BaseServer, JobGetterMixin):

    redis_client = None

    def __init__(self, config):
        super(CassandraServer, self).__init__(config)
        self.cassandra_cf_content = config["cassandra_cf_content"]
        self.cassandra_cf_temp_content = config["cassandra_cf_temp_content"]
        # Cassandra Clients & Factorys
        factory = ManagedCassandraClientFactory(config["cassandra_keyspace"])
        self.cassandra_client = CassandraClient(factory)
        reactor.connectTCP(
            config["cassandra_server"],
            config.get("cassandra_port", 9160),
            factory)
        # Negative Cache
        self.disable_negative_cache = config.get("disable_negative_cache", False)
        # Redis
        self.redis_hosts = config["redis_hosts"]
        # deltas
        self.delta_log_enabled = config.get('delta_log_enabled', False)
        self.delta_log_path = config.get('delta_log_path', '/tmp/deltas/')
        self.delta_enabled = config.get('delta_enabled', False)
        self.delta_debug = config.get('delta_debug', False)
        # create the log path if required & enabled
        if self.delta_enabled and self.delta_log_enabled and not os.path.exists(self.delta_log_path):
            os.makedirs(self.delta_log_path)
        self.cassandra_cf_delta = config.get('cassandra_cf_delta', None)
        self.cassandra_cf_delta_user = config.get('cassandra_cf_delta_user', None)
        # sanity check config;  if cfs aren't set, turn deltas off
        if not all([self.cassandra_cf_delta, self.cassandra_cf_delta_user]):
            logger.warn('Disabling cassandra deltas; both cf_delta and'
                ' cf_delta_user must be set in the config.')
            self.delta_enabled = False
        self.setupJobGetter(config)

    def start(self):
        start_deferred = super(CassandraServer, self).start()
        start_deferred.addCallback(self._cassandraStart)
        return start_deferred

    @inlineCallbacks
    def _cassandraStart(self, started=False):
        logger.debug("Starting Cassandra components.")
        try:
            self.redis_client = yield RedisShardingConnection(self.redis_hosts)
        except Exception, e:
            logger.error("Could not connect to Redis: %s" % e)
            self.shutdown()
            raise Exception("Could not connect to Redis.")
        if self.disable_negative_cache:
            logger.warning("Disabling negative cache.")
        self.pg = PageGetter(
            self.cassandra_client,
            redis_client=self.redis_client,
            disable_negative_cache=self.disable_negative_cache,
            rq=self.rq)
        returnValue(True)

    def shutdown(self):
        # Shutdown things here.
        return super(CassandraServer, self).shutdown()

    def logDelta(self, uuid, old, new, delta):
        """Log a delta in a way we can examine later or incorporate into our
        unit testing corpus."""
        if not self.delta_log_enabled: return
        basepath = os.path.join(self.delta_log_path, uuid)
        with open(basepath + '.old.js', 'w') as f:
            f.write(simplejson.dumps(old, indent=2))
        with open(basepath + '.new.js', 'w') as f:
            f.write(simplejson.dumps(new, indent=2))
        with open(basepath + '.res.js', 'w') as f:
            f.write(simplejson.dumps(delta, indent=2))

    def iterate_deltas(self, deltas):
        if isinstance(deltas, dict):
            for key,value in delta.iteritems():
                yield key, value
        else:
            for data in deltas:
                ts = time.time()
                delta_uuid = '%0.2f:%s' % (ts, uuid4().hex)
                yield delta_uuid, data

    @inlineCallbacks
    def executeJob(self, job):
        user_id = job.user_account["user_id"]
        new_data = yield super(CassandraServer, self).executeJob(job)
        if new_data is None:
            return
        if self.delta_enabled:
            delta_func = self.functions[job.function_name]["delta"]
            if delta_func is not None:
                service = job.subservice.split('/')[0]
                old_data = yield self.getData(user_id, job.uuid)
                # TODO: make sure we check that old_data exists
                delta = delta_func(new_data, old_data)
                self.logDelta(job.uuid, old_data, new_data, delta)
                logger.debug("Got delta: %s ..." % str(delta)[:1000])
                for delta_id, data in self.iterate_deltas(delta):
                    category = self.functions[job.function_name]['category']
                    service = job.subservice.split('/')[0]
                    user_column = '%s:%s:%s' % (delta_id, category, job.subservice)
                    logger.info("Inserting delta id %s to user_column: %s" % (str(delta_id), user_column))
                    mapping = {
                        'data': zlib.compress(simplejson.dumps(data)),
                        'user_id': str(user_id),
                        'category': category,
                        'service': service,
                        'subservice': job.subservice,
                        'uuid': job.uuid}
                    if self.delta_debug:
                        ts = str(time.time())
                        mapping.update({
                            'old_data': zlib.compress(simplejson.dumps(old_data)),
                            'new_data': zlib.compress(simplejson.dumps(new_data)),
                            'generated': ts,
                            'updated': ts})
                    yield self.cassandra_client.batch_insert(
                        key=str(delta_id),
                        column_family=self.cassandra_cf_delta,
                        mapping=mapping)
                    yield self.cassandra_client.insert(
                        key=str(user_id),
                        column_family=self.cassandra_cf_delta_user,
                        column=user_column,
                        value='')
        yield self.cassandra_client.insert(
            str(user_id),
            self.cassandra_cf_content,
            zlib.compress(simplejson.dumps(new_data)),
            column=job.uuid)
        returnValue(new_data)

    @inlineCallbacks
    def getData(self, user_id, uuid):
        data = yield self.cassandra_client.get(
            key=str(user_id),
            column_family=self.cassandra_cf_content,
            column=uuid)
        returnValue(simplejson.loads(zlib.decompress(data.column.value)))

    @inlineCallbacks
    def deleteReservation(self, uuid):
        """Delete a reservation by uuid."""
        # FIXME: this function is unnecessarily coupled to the job object;
        # only a uuid is needed to delete a reservation
        logger.info('Deleting UUID from spider_service table: %s' % uuid)
        yield self.mysql.runQuery('DELETE FROM spider_service WHERE uuid=%s', uuid)
        url = 'http://%s:%s/function/schedulerserver/remoteremovefromheap?%s' % (
            self.scheduler_server,
            self.scheduler_server_port,
            urllib.urlencode({'uuid': uuid}))
        logger.info('Sending UUID to scheduler to be dequeued: %s' % url)
        try:
            yield self.rq.getPage(url=url)
        except Exception, e:
            tb = traceback.format_exc()
            logger.error("failed to deque job %s on scheduler (url was: %s):\n%s" % (
                uuid, url, tb))
            # TODO: punt here?
        logger.info('Deleting UUID from Cassandra: %s' % uuid)
        yield self.cassandra_client.remove(
            uuid,
            self.cassandra_cf_content)
        returnValue({'success':True})
        
    @inlineCallbacks
    def regenerate_delta(self, delta_id):
        # Get the delta data, the old data, the new data, and the subservice.
        data = yield self.cassandra_client.get_slice(
            key=delta_id,
            names=["data", "old_data", "new_data", "subservice"],
            column_family=self.cassandra_cf_delta)
        row = dict([(x.column.name, x.column.value) for x in data])
        new_data = simplejson.loads(zlib.decompress(row["new_data"]))
        old_data = simplejson.loads(zlib.decompress(row["old_data"]))
        delta_func = self.functions[row["subservice"]]["delta"]
        # Generate deltas.
        deltas = list(self.iterate_deltas(delta_func(new_data, old_data)))
        # If no delta exists, clear the old data out.
        if len(deltas) == 0:
            replacement_delta = ""
            yield self.cassandra_client.batch_insert(
                key=delta_id,
                column_family=self.cassandra_cf_delta,
                mapping={
                    "data":"",
                    "updated":str(time.time())
                })
        # If one delta exists, replace the old data with the new delta.
        elif len(deltas) == 1:
            replacement_delta = simplejson.dumps(deltas[0][1])
            yield self.cassandra_client.batch_insert(
                key=delta_id,
                column_family=self.cassandra_cf_delta,
                mapping={
                    "data":zlib.compress(replacement_delta),
                    "updated":str(time.time())
                })
        # If multiple deltas exists, replace them with the closest match.
        else:
            delta_options = []
            # Generate tuples of (similiarity ratio, JSON formatted data)
            s = SequenceMatcher()
            s.set_seq1(zlib.decompress(row["data"]))
            for k, v in deltas:
                value = simplejson.dumps(v)
                s.set_seq2(value)
                delta_options.append((s.ratio(), value))
            # Sort to find the most similar option.
            value = sorted(delta_options, key=lambda x:x[0]).pop()[1]
            replacement_delta = value
            yield self.cassandra_client.batch_insert(
                key=delta_id,
                column_family=self.cassandra_cf_delta,
                mapping={
                    "data":zlib.compress(value),
                    "updated":str(time.time())})
        returnValue({
            'replacement_delta':replacement_delta,
            'deltas':simplejson.dumps([x[1] for x in deltas])})
