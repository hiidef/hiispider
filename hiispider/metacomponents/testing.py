#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Executes jobs by UUID."""

from traceback import format_exc
from twisted.internet.defer import inlineCallbacks, returnValue, maybeDeferred
from hiispider.components import *
from hiispider.metacomponents import *
import logging
from copy import copy
from hiispider.job import Job
from .base import MetaComponent

LOGGER = logging.getLogger(__name__)


class Testing(MetaComponent):

    allow_clients = False
    requires = [MySQL, PageGetter, Cassandra]

    def __init__(self, server, config, server_mode, **kwargs):
        super(Testing, self).__init__(server, server_mode)
        self.config = copy(config)
        self.server.expose(self.execute_by_uuid)

    def initialize(self):
        self.server.pagegetter.disableNegativeCache()

    @inlineCallbacks
    def execute_by_uuid(self, uuid):
        sql = """SELECT uuid, content_userprofile.user_id as user_id,
                username, host, account_id, type
            FROM spider_service, auth_user, content_userprofile
            WHERE uuid IN ('%s')
            AND auth_user.id=spider_service.user_id
            AND auth_user.id=content_userprofile.user_id
            """ % uuid
        try:
            data = yield self.server.mysql.runQuery(sql)
        except Exception, e:
            LOGGER.error("Could not find users in '%s': %s" % (uuid, e))
            raise
        user_account = data[0]
        sql = "SELECT * FROM content_%saccount WHERE account_id IN (%s)" % (
            user_account["type"].split("/")[0],
            user_account["account_id"])
        try:
            data = yield self.server.mysql.runQuery(sql)
        except Exception, e:
            LOGGER.error("Could not find service %s, %s: %s" % (
                user_account["type"],
                sql, e))
            raise
        service_credentials = data[0]
        job = Job(
            function_name=user_account['type'],
            service_credentials=service_credentials,
            user_account=user_account,
            uuid=user_account['uuid'])
        self.server.worker.mapJob(job)
        LOGGER.debug(job)
        f = self.server.functions[job.function_name]
        try:
            data = yield maybeDeferred(f['function'], **job.kwargs)
        except Exception, e:
            if hasattr(e, "response"):
                LOGGER.error(e.response)
            LOGGER.error("Error executing job: %s" % format_exc())
            raise
        returnValue(data)
