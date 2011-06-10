from uuid import UUID, uuid4
import time
import random
import logging
import logging.handlers
import random

from twisted.internet import reactor, task
from twisted.web import server
from twisted.enterprise import adbapi
from MySQLdb.cursors import DictCursor
from twisted.internet.defer import Deferred, inlineCallbacks, DeferredList
from twisted.internet import task
from twisted.internet.threads import deferToThread
from txamqp.content import Content
from .base import LOGGER
from .cassandra import CassandraServer
from twisted.web.resource import Resource

class CassandraTestingServer(CassandraServer):

    name = "Cassandra Testing Server UUID: %s" % str(uuid4())

    def __init__(self,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            cassandra_server=None,
            cassandra_port=9160,
            cassandra_keyspace=None,
            cassandra_stats_keyspace=None,
            cassandra_stats_cf_daily=None,
            cassandra_cf_content=None,
            cassandra_content=None,
            cassandra_content_error='error',
            cassandra_error='error',
            mysql_username=None,
            mysql_password=None,
            mysql_host=None,
            mysql_database=None,
            mysql_port=3306,
            redis_hosts=None,
            disable_negative_cache=True,
            port=5002,
            service_mapping=None,
            service_args_mapping=None,
            log_file='testingserver.log',
            log_directory=None,
            log_level="debug"):
        self.function_resource = Resource()
        # Create MySQL connection.
        self.mysql = adbapi.ConnectionPool(
            "MySQLdb",
            db=mysql_database,
            port=mysql_port,
            user=mysql_username,
            passwd=mysql_password,
            host=mysql_host,
            cp_reconnect=True,
            cursorclass=DictCursor)
        # Resource Mappings
        self.service_mapping = service_mapping
        self.service_args_mapping = service_args_mapping
        # HTTP interface
        resource = Resource()
        self.function_resource = Resource()
        resource.putChild("function", self.function_resource)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Logging, etc
        CassandraServer.__init__(
            self,
            cassandra_server=cassandra_server,
            cassandra_port=cassandra_port,
            cassandra_keyspace=cassandra_keyspace,
            cassandra_stats_keyspace=cassandra_stats_keyspace,
            cassandra_stats_cf_daily=cassandra_stats_cf_daily,
            cassandra_cf_content=cassandra_cf_content,
            cassandra_content=cassandra_content,
            cassandra_content_error=cassandra_content_error,
            cassandra_error=cassandra_error,
            redis_hosts=redis_hosts,
            disable_negative_cache=disable_negative_cache,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level)
        self.expose(self.listUUIDs)
        self.expose(self.executeUUID)

    def start(self):
        reactor.callWhenRunning(self._start)
        return self.start_deferred

    def _start(self):
        # Load in names of functions supported by plugins
        self.function_names = self.functions.keys()

    @inlineCallbacks
    def shutdown(self):
        LOGGER.info('Closing MYSQL Connnection Pool')
        yield self.mysql.close()

    def listUUIDs(self, username):
        sql = """SELECT spider_service.type, spider_service.uuid
                 FROM spider_service
                 INNER JOIN auth_user ON spider_service.user_id = auth_user.id
                 WHERE auth_user.username = %s
              """
        LOGGER.debug(sql)
        d = self.mysql.runQuery(sql, username)
        d.addCallback(self._listUUIDsCallback)
        return d

    def _listUUIDsCallback(self, data):
        return data

    def _genericErrback(self, error, type):
        LOGGER.error("%s - %s" % (type, error))

    def executeUUID(self, uuid):
        sql = "SELECT account_id, type, user_id FROM spider_service WHERE uuid = %s"
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
        LOGGER.debug('No spider_info given for uuid %s' % uuid)
        return None

    def _getJobCallback2(self, account_info, spider_info, uuid):
        account = account_info[0]
        function_name = spider_info[0]['type']
        job = {}
        job['type'] = function_name.split('/')[1]
        job['subservice_name'] = function_name
        if self.service_mapping and self.service_mapping.has_key(function_name):
            LOGGER.debug('Remapping resource %s to %s' % (function_name, self.service_mapping[function_name]))
            function_name = self.service_mapping[function_name]
        job['exposed_function'] = self.functions[function_name]
        job['function_name'] = function_name
        job['uuid'] = uuid
        job['account'] = account
        job["kwargs"] = self.mapKwargs(job)
        if function_name not in self.functions:
            raise Exception("Function %s does not exist." % function_name)
        print "Key: %s, CF: %s, Col: %s" % (
            str(spider_info[0]['user_id']),
            self.cassandra_cf_content,
            uuid
        )
        if self.functions[function_name]["delta"] is not None:
            d = self.getData(str(spider_info[0]['user_id']), uuid)
            d.addCallback(self._getJobCallback3, function_name, job, spider_info)
            d.addErrback(self._getJobErrback2, uuid, function_name, job, spider_info)
        else:
            d = self.callExposedFunction(
                self.functions[function_name]["function"],
                job["kwargs"],
                function_name)
        return d
    
    def _getJobErrback2(self, error, function_name, job, spider_info):
        LOGGER.error("%s:%s - %s" % (function_name, uuid, error))
        d = self.callExposedFunction(
            self.functions[function_name]["function"],
            job["kwargs"],
            function_name)
        return d

    def _getJobCallback3(self, old_data, function_name, job, spider_info):
        d = self.callExposedFunction(
            self.functions[function_name]["function"],
            job["kwargs"],
            function_name)
        d.addCallback(self._getJobCallback4, old_data, function_name)
        d.addErrback(self._genericErrback, spider_info[0]['type'])
        return d
    
    def _getJobCallback4(self, new_data, old_data, function_name):
        delta = self.functions[function_name]["delta"](old_data, new_data)
        print delta
        return new_data
    
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
        LOGGER.debug('Function: %s\nKWARGS: %s' % (job['function_name'], repr(kwargs)))
        return kwargs

