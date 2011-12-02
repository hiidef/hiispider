#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Executes new jobs on behalf of the Django application."""

from twisted.internet.defer import inlineCallbacks, returnValue
from hiispider.components import *
from hiispider.metacomponents import *
import logging
from twisted.internet import reactor
from traceback import format_exc
from .identity import Identity
import cPickle
from zlib import compress

LOGGER = logging.getLogger(__name__)


class IdentityInterface(Identity):

    allow_clients = False
    requires = [MySQL, Cassandra, Logger, Redis, PageGetter]

    def __init__(self, server, config, server_mode, **kwargs):
        super(IdentityInterface, self).__init__(server, config, server_mode, **kwargs)
        self.server.expose(self.generate_recomendations)
        self.server.expose(self.connect)

    @inlineCallbacks
    def connect(self, user_id, account_type):
        if "custom_" in account_type:
            return
        sql = """SELECT content_%(account_type)saccount.*, content_account.user_id
        FROM content_%(account_type)saccount
        INNER JOIN content_account
        ON content_%(account_type)saccount.account_id = content_account.id
        WHERE content_account.user_id = %(user_id)s""" % {
            "account_type": account_type,
            "user_id": user_id}
        mapping = self.inverted_args_mapping.get(account_type, {})
        results = yield self.server.mysql.runQuery(sql)
        for row in results:
            row["_account_type"] = account_type
            for key, value in mapping.iteritems():
                if value in row:
                    row[key] = row.pop(value)
            row["user_id"] = str(row["user_id"])
            self.server.redis.set(row["user_id"], compress(cPickle.dumps(row)))
            yield self.get_service_identity(row)
            yield self.get_service_connections(row)
        connections = yield self.server.cassandra.getServiceConnections(account_type, user_id)
        inserts = []
        for followee_id in connections.values():                    
            sql = ("INSERT IGNORE INTO streams_follow (follower_id, followee_id, "
                "date_added, is_blocked) VALUES (%s, %s, now(), 0)" % (
                    user_id,
                    followee_id
                ))
            LOGGER.debug(sql)
            inserts.append(sql)
        if inserts:
            yield self.server.mysql.runQuery(";".join(inserts) + ";")
        returnValue({"success":True, "connections":len(inserts)})

    def generate_recomendations(self):
        reactor.callLater(0, self._generate_recomendations)
        return "Generating recommendations."

    @inlineCallbacks
    def _generate_recomendations(self):
        start = ''
        inserts = []
        while True:
            recommendations = yield self.server.cassandra.get_range_slices(
                column_family=self.server.cassandra.cf_recommendations,
                start=start,
                count=100)
            for row in recommendations:
                follower_id = row.key
                followers = [(x.counter_column.name, x.counter_column.value) for x in row.columns]
                followers = sorted(followers, key=lambda x: x[1], reverse=True)[0:100]
                for followee_id, value in followers:
                    sql = ("INSERT IGNORE INTO streams_follow (follower_id, followee_id, "
                        "date_added, is_blocked) VALUES (%s, %s, now(), 0)" % (
                            follower_id,
                            followee_id
                        ))
                    inserts.append(sql)
            try:
                yield self.server.mysql.runOperation(";".join(inserts) + ";")
                LOGGER.debug("Inserted %s rows." % len(inserts))
            except:
                LOGGER.error(format_exc())
            inserts = []
            start = follower_id
            if len(recommendations) < 100 and self.running:
                break
        yield self.server.mysql.runQuery(";".join(inserts) + ";")