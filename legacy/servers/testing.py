#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Testing server, runs uuids like a workerserver but only on http req."""

from uuid import uuid4
import logging

from MySQLdb.cursors import DictCursor
from twisted.internet import reactor
from twisted.web import server
from twisted.enterprise import adbapi
from twisted.internet.defer import inlineCallbacks
from twisted.web.resource import Resource

from hiispider.servers.base import BaseServer
from hiispider.servers.mixins.mysql import MySQLMixin

logger = logging.getLogger(__name__)

class TestingServer(BaseServer, MySQLMixin):

    name = "Testing Server UUID: %s" % str(uuid4())

    def __init__(self, config, port=5002):
        BaseServer.__init__(self, config)
        self.setupMySQL(config)
        port = int(port)
        self.function_resource = Resource()
        # HTTP interface
        resource = Resource()
        self.function_resource = Resource()
        resource.putChild("function", self.function_resource)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Logging, etc
        self.expose(self.listUUIDs)
        self.expose(self.executeUUID)

    def start(self):
        start_deferred = super(TestingServer, self).start()
        reactor.callWhenRunning(self._start)
        return start_deferred

    def _start(self):
        # Load in names of functions supported by plugins
        self.function_names = self.functions.keys()

    @inlineCallbacks
    def shutdown(self):
        logger.info('Closing MYSQL Connnection Pool')
        yield self.mysql.close()

    def listUUIDs(self, username):
        sql = """SELECT spider_service.type, spider_service.uuid
                 FROM spider_service
                 INNER JOIN auth_user ON spider_service.user_id = auth_user.id
                 WHERE auth_user.username = %s
              """
        logger.debug(sql)
        d = self.mysql.runQuery(sql, username)
        d.addCallback(self._listUUIDsCallback)
        return d

    def _listUUIDsCallback(self, data):
        return data

    def _genericErrback(self, error, type):
        logger.error("%s - %s" % (type, error))

    def executeUUID(self, uuid):
        sql = "SELECT account_id, type FROM spider_service WHERE uuid = %s"
        d = self.mysql.runQuery(sql, uuid)
        d.addCallback(self._getJobCallback, uuid)
        d.addErrback(self._genericErrback, 'Get Job Callback')
        return d

    def _getJobCallback(self, spider_info, uuid):
        if spider_info:
            account_type = spider_info[0]['type'].split('/')[0]
            sql = "SELECT * FROM content_%saccount WHERE account_id = %d" % (account_type.lower(), spider_info[0]['account_id'])
            d = self.mysql.runQuery(sql)
            d.addCallback(self._getJobCallback2, spider_info, uuid)
            d.addErrback(self._genericErrback, 'Get MySQL Account')
            return d
        logger.debug('No spider_info given for uuid %s' % uuid)
        return None

    def _getJobCallback2(self, account_info, spider_info, uuid):
        account = account_info[0]
        function_name = spider_info[0]['type']
        job = {}
        job['type'] = function_name.split('/')[1]
        if self.service_mapping and function_name in self.service_mapping:
            logger.debug('Remapping resource %s to %s' % (function_name, self.service_mapping[function_name]))
            function_name = self.service_mapping[function_name]
        job['exposed_function'] = self.functions[function_name]
        job['function_name'] = function_name
        job['uuid'] = uuid
        job['account'] = account
        job["kwargs"] = self.mapKwargs(job)
        if function_name not in self.functions:
            raise Exception("Function %s does not exist." % function_name)
        d = self.callExposedFunction(
            self.functions[function_name]["function"],
            job["kwargs"],
            function_name)
        return d

    def mapKwargs(self, job):
        kwargs = {}
        service_name = job['function_name'].split('/')[0]
        # remap some fields that differ from the plugin and the database
        if service_name in self.service_args_mapping:
            for key in self.service_args_mapping[service_name]:
                if key in job['account']:
                    job['account'][self.service_args_mapping[service_name][key]] = job['account'][key]
        # apply job fields to req and optional kwargs
        exposed_function = self.functions[job['function_name']]
        for arg in exposed_function['required_arguments']:
            if arg in job:
                kwargs[str(arg)] = job[arg]
            elif arg in job['account']:
                kwargs[str(arg)] = job['account'][arg]
        for arg in exposed_function['optional_arguments']:
            if arg in job['account']:
                kwargs[str(arg)] = job['account'][arg]
        logger.debug('Function: %s\nKWARGS: %s' % (job['function_name'], repr(kwargs)))
        return kwargs
