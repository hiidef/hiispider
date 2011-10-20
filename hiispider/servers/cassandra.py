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
import random

from uuid import uuid4
from difflib import SequenceMatcher, unified_diff
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList
from twisted.internet import reactor
from .base import BaseServer
from ..pagegetter import PageGetter
from ..delta import Autogenerator, Delta
from txredisapi import RedisShardingConnection
from mixins.jobgetter import JobGetterMixin
from ..uuidhelpers import convert_time_to_uuid
from telephus.cassandra.c08.ttypes import IndexExpression, IndexOperator, NotFoundException
from telephus.pool import CassandraClusterPool
from hiispider.exceptions import *

PP = pprint.PrettyPrinter(indent=4)
logger = logging.getLogger(__name__)


def recursive_sort(x):
    if isinstance(x, basestring):
        return x
    elif isinstance(x, list):
        y = []
        for value in x:
            value = recursive_sort(value)
            if isinstance(value, dict):
                y.append((PP.pformat(value), value))
            else:
                y.append((value, value))
        return [a[1] for a in sorted(y, key=lambda b:b[0])]
    elif isinstance(x, dict):
        for key in x:
            x[key] = recursive_sort(x[key])
        return x
    else:
        try:
            return sorted(x)
        except TypeError:
            return x


class CassandraServer(BaseServer, JobGetterMixin):

    regenerating = False
    redis_client = None

    def __init__(self, config):
        super(CassandraServer, self).__init__(config)
        self.cassandra_cf_content = config["cassandra_cf_content"]
        self.cassandra_cf_temp_content = config["cassandra_cf_temp_content"]
        # Cassandra Clients & Factorys
        self.cassandra_client = CassandraClusterPool(
            config["cassandra_servers"],
            keyspace=config["cassandra_keyspace"],
            pool_size=len(config["cassandra_servers"]) * 2)
        self.cassandra_client.startService()
        # Negative Cache
        self.disable_negative_cache = config.get("disable_negative_cache", False)
        # Redis
        self.redis_hosts = config["redis_hosts"]
        # deltas
        self.delta_enabled = config.get('delta_enabled', False)
        self.delta_debug = config.get('delta_debug', False)
        self.delta_sample_rate = config.get('delta_sample_rate', 1.0)
        # create the log path if required & enabled
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
        logger.debug("Started RedisShardingConnection")
        self.pg = PageGetter(
            self.cassandra_client,
            redis_client=self.redis_client,
            disable_negative_cache=self.disable_negative_cache,
            rq=self.rq)
        returnValue(True)

    def shutdown(self):
        # Shutdown things here.
        return super(CassandraServer, self).shutdown()

    def deltaSampleRate(self):
        return random.random() <= self.delta_sample_rate

    @inlineCallbacks
    def executeJob(self, job):
        user_id = job.user_account["user_id"]
        new_data = yield super(CassandraServer, self).executeJob(job)
        if new_data is None:
            return
        if self.delta_enabled and self.deltaSampleRate():
            delta_func = self.functions[job.function_name]["delta"]
            if delta_func is not None:
                service = job.subservice.split('/')[0]
                old_data = yield self.getData(user_id, job.uuid)
                # TODO: make sure we check that old_data exists
                deltas = delta_func(new_data, old_data)
                logger.debug("Got %s deltas" % len(deltas))
                for delta in deltas:
                    # FIXME why is category not always set?
                    category = 'unknown'
                    if self.functions[job.function_name]['category']:
                        category = self.functions[job.function_name]['category']
                    service = job.subservice.split('/')[0]
                    user_column = b'%s||%s||%s||%s' % (delta.id, category, job.subservice, str(job.user_account['account_id']))
                    mapping = {
                        'data': zlib.compress(simplejson.dumps(delta.data)),
                        'user_id': str(user_id),
                        'category': category,
                        'service': service,
                        'subservice': job.subservice,
                        'uuid': job.uuid,
                        "path": delta.path}
                    if self.delta_debug:
                        ts = str(time.time())
                        mapping.update({
                            'old_data': zlib.compress(simplejson.dumps(old_data)),
                            'new_data': zlib.compress(simplejson.dumps(new_data)),
                            'generated': ts,
                            'updated': ts})
                    yield self.cassandra_client.batch_insert(
                        key=str(delta.id),
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
    def saveData(self, user_id, uuid, data):
        """Save data in cassandra.  The opposite of ``getData``."""
        ret = yield self.cassandra_client.insert(
            str(user_id),
            self.cassandra_cf_content,
            zlib.compress(simplejson.dumps(data)),
            column=uuid)
        returnValue(ret)


    @inlineCallbacks
    def getData(self, user_id, uuid):
        """Get data from cassandra by user id and uuid."""
        try:
            data = yield self.cassandra_client.get(
                key=str(user_id),
                column_family=self.cassandra_cf_content,
                column=uuid)
            returnValue(simplejson.loads(zlib.decompress(data.column.value)))
        except NotFoundException:
            returnValue([])

    @inlineCallbacks
    def deleteReservation(self, uuid):
        """Delete a reservation by uuid."""
        logger.info('Deleting UUID from spider_service table: %s' % uuid)
        yield self.mysql.runQuery('DELETE FROM spider_service WHERE uuid=%s', uuid)
        url = 'http://%s:%s/function/schedulerserver/removefromjobsheap?%s' % (
            self.scheduler_server,
            self.scheduler_server_port,
            urllib.urlencode({'uuid': uuid}))
        logger.info('Sending UUID to scheduler to be dequeued: %s' % url)
        try:
            yield self.rq.getPage(url=url)
        except Exception:
            tb = traceback.format_exc()
            logger.error("failed to deque job %s on scheduler"
                " (url was: %s):\n%s" % (uuid, url, tb))
            # TODO: punt here?
        logger.info('Deleting UUID from Cassandra: %s' % uuid)
        yield self.cassandra_client.remove(
            uuid,
            self.cassandra_cf_content)
        returnValue({'success': True})

    def regenerate_deltas(self, service_type=None):
        if self.regenerating:
            return {"success": False, "message": "Already regenerating. Come back later."}
        self.regenerating = True
        reactor.callLater(
            0,
            self._regenerate_deltas,
            service_type=service_type)
        return {"success": True, "message": "Queuing deltas to be regenerated."}

    @inlineCallbacks
    def _regenerate_deltas(self, start='', count=0, service_type=None):
        try:
            if service_type:
                expressions = [IndexExpression('subservice', IndexOperator.EQ, service_type)]
                range_slice = yield self.cassandra_client.get_indexed_slices(
                    column_family=self.cassandra_cf_delta,
                    expressions=expressions,
                    column_count=1,
                    start_key=start,
                    count=100)
            else:
                range_slice = yield self.cassandra_client.get_range_slices(
                    column_family=self.cassandra_cf_delta,
                    column_count=0,
                    start=start,
                    count=100)
            deferreds = []
            items_regenerated = 0
            for x in range_slice:
                items_regenerated += 1
                last_key = x.key
                d = self.regenerate_delta(x.key)
                d.addErrback(self._regenerateErrback)
                deferreds.append(d)
            yield DeferredList(deferreds, consumeErrors=True)
            count += items_regenerated
            logger.info("Regenerated %s(%s) deltas." % (items_regenerated, count))
        except Exception, e:
            logger.error("Regenerating deltas failed: %s" % e)
            self.regenerateing = False
        if items_regenerated >= 100:
            reactor.callLater(0, self._regenerate_deltas, start=last_key, count=count, service_type=service_type)
        else:
            logging.info("Regenerating deltas complete")
            self.regenerating = False

    def _regenerateErrback(self, error):
        logger.error(str(error))

    @inlineCallbacks
    def delete_delta(self, delta_id, user_id=None):
        logger.debug("Deleting %s" % delta_id.encode('hex'))
        yield self.cassandra_client.remove(
            key=delta_id,
            column_family=self.cassandra_cf_delta)
        if user_id:
            yield self.cassandra_client.remove(
                key=user_id,
                column_family=self.cassandra_cf_delta_user,
                column=delta_id)

    @inlineCallbacks
    def regenerate_delta(
            self,
            delta_id,
            paths=None,
            includes=None,
            ignores=None,
            dates=None,
            return_new_keys=False):
        # Get the delta data, the old data, the new data, and the subservice.
        data = yield self.cassandra_client.get_slice(
            key=delta_id,
            names=["data", "old_data", "new_data", "service", "category", "subservice", "user_id"],
            column_family=self.cassandra_cf_delta)
        row = dict([(x.column.name, x.column.value) for x in data])
        try:
            user_id = row["user_id"] or None
        except KeyError:
            yield self.delete_delta(delta_id)
            return
        try:
            new_data = simplejson.loads(zlib.decompress(row["new_data"]))
            old_data = simplejson.loads(zlib.decompress(row["old_data"]))
        except KeyError:
            yield self.delete_delta(delta_id, user_id)
            return
        delete_on_empty = False
        if not all([x is None for x in (paths, includes, ignores, dates)]):
            if paths:
                paths = paths.split(",")
            if includes:
                includes = includes.split(",")
            if ignores:
                ignores = ignores.split(",")
            if dates:
                dates = dates.split(",")
            delta_func = Autogenerator(
                paths,
                ignores,
                includes,
                dates,
                bool(int(return_new_keys)))
            logger.debug("Using custom Autogenerator.")
        else:
            if row["subservice"] in self.service_mapping:
                logger.debug('remapped service %s to %s for delta %s' % (row["subservice"],
                    self.service_mapping[row["subservice"]],
                    delta_id.encode('hex')))
                delta_func = self.functions[self.service_mapping[row["subservice"]]]["delta"]
            else:
                delta_func = self.functions[row["subservice"]]["delta"]
            if not delta_func:
                logger.warn('No delta_func for delta %s found. Setting delta_func to Autogenerator' % delta_id.encode('hex'))
                delta_func = Autogenerator(
                    paths,
                    ignores,
                    includes,
                    dates,
                    bool(int(return_new_keys)))
            # In the event of a stock Autogenerator, remove on empty.
            if isinstance(delta_func, Autogenerator):
                if len(delta_func.paths) == 1:
                    if not delta_func.paths[0]["path"] and not \
                            delta_func.paths[0]["includes"] and not \
                            delta_func.paths[0]["ignores"]:
                        delete_on_empty = True
        # Generate deltas.
        logger.debug('Regenerating delta %s using delta_func: %s' % (delta_id.encode('hex'), type(delta_func)))
        deltas = delta_func(new_data, old_data)
        # If no delta exists, clear the old data out.
        if len(deltas) == 0:
            if delete_on_empty:
                replacement_delta = None
                yield self.delete_delta(delta_id, user_id)
            else:
                replacement_delta = None
                yield self.cassandra_client.batch_insert(
                    key=delta_id,
                    column_family=self.cassandra_cf_delta,
                    mapping={
                        "data": zlib.compress(simplejson.dumps("")),
                        "updated": str(time.time()),
                        "service": row["service"],
                        "category": row["category"],
                        "subservice": row["subservice"],
                    })
                logger.debug("DELTA %s\nEmpty delta." % delta_id.encode('hex'))
        # If one delta exists, replace the old data with the new delta.
        elif len(deltas) == 1:
            replacement_delta = deltas[0].data
            yield self.cassandra_client.batch_insert(
                key=delta_id,
                column_family=self.cassandra_cf_delta,
                mapping={
                    "data": zlib.compress(simplejson.dumps(replacement_delta)),
                    "updated": str(time.time()),
                    "service": row["service"],
                    "category": row["category"],
                    "subservice": row["subservice"],

                })
            logger.debug("DELTA %s\nOne result:\n%s" % (
                delta_id.encode('hex'),
                PP.pformat(replacement_delta)))
        # If multiple deltas exists, replace them with the closest match.
        else:
            delta_options = []
            # Generate tuples of (similiarity ratio, JSON formatted data)
            s = SequenceMatcher()
            s.set_seq1(zlib.decompress(row["data"]))
            for delta in deltas:
                value = simplejson.dumps(delta.data)
                s.set_seq2(value)
                delta_options.append((s.ratio(), value, delta.data))
            # Sort to find the most similar option.
            delta_options = sorted(delta_options, key=lambda x: x[0])
            replacement_delta = delta_options[-1][2]
            yield self.cassandra_client.batch_insert(
                key=delta_id,
                column_family=self.cassandra_cf_delta,
                mapping={
                    "data": zlib.compress(simplejson.dumps(replacement_delta)),
                    "updated": str(time.time()),
                    "service": row["service"],
                    "category": row["category"],
                    "subservice": row["subservice"]})
            logger.debug("DELTA %s\nMultiple results:\n%s" % (
                delta_id.encode('hex'),
                PP.pformat([x[2] for x in delta_options])))
        logger.debug("DIFF: " + "\n".join(list(unified_diff(
            PP.pformat(recursive_sort(old_data)).split("\n"),
            PP.pformat(recursive_sort(new_data)).split("\n")))))
        returnValue({
            'replacement_delta': replacement_delta,
            'deltas': [x.data for x in deltas]})
