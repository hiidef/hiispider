import pprint
import urllib
from uuid import uuid4
from MySQLdb.cursors import DictCursor
from twisted.enterprise import adbapi
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred
from twisted.internet.threads import deferToThread
from twisted.web.resource import Resource
from twisted.internet import reactor
from twisted.web import server
from .cassandra import CassandraServer
from .base import LOGGER, SmartConnectionPool
from ..resources import InterfaceResource
from ..evaluateboolean import evaluateBoolean
import zlib

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)

class InterfaceServer(CassandraServer):

    name = "HiiSpider Interface Server UUID: %s" % str(uuid4())
    
    def __init__(self,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            cassandra_server=None, 
            cassandra_keyspace=None,
            cassandra_cf_content=None,
            cassandra_content=None,
            redis_hosts=None,
            disable_negative_cache=True,
            scheduler_server=None,
            scheduler_server_port=5001,
            max_simultaneous_requests=50,
            max_requests_per_host_per_second=1,
            max_simultaneous_requests_per_host=5,
            port=5000, 
            log_file='interfaceserver.log',
            log_directory=None,
            log_level="debug",
            mysql_username=None,
            mysql_password=None,
            mysql_host=None,
            mysql_database=None,
            mysql_port=3306,
            ):
        # Create MySQL connection.
        self.mysql = SmartConnectionPool(
            "MySQLdb",
            db=mysql_database,
            port=mysql_port,
            user=mysql_username,
            passwd=mysql_password,
            host=mysql_host,
            cp_reconnect=True,
            cursorclass=DictCursor)
        # Cassandra
        self.cassandra_server=cassandra_server
        self.cassandra_keyspace=cassandra_keyspace
        self.cassandra_cf_content=cassandra_cf_content
        self.cassandra_content=cassandra_content
        # Redis
        self.redis_hosts = redis_hosts
        # Negative Cache Disabled?
        self.disable_negative_cache = disable_negative_cache
        self.aws_access_key_id=aws_access_key_id
        self.aws_secret_access_key=aws_secret_access_key
        resource = Resource()
        interface_resource = InterfaceResource(self)
        resource.putChild("interface", interface_resource)
        self.function_resource = Resource()
        resource.putChild("function", self.function_resource)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        CassandraServer.__init__(
            self,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            cassandra_server=cassandra_server,
            cassandra_keyspace=cassandra_keyspace,
            cassandra_cf_content=cassandra_cf_content,
            cassandra_content=cassandra_content,
            redis_hosts=redis_hosts,
            disable_negative_cache=disable_negative_cache,
            max_simultaneous_requests=max_simultaneous_requests,
            max_requests_per_host_per_second=max_requests_per_host_per_second,
            max_simultaneous_requests_per_host=max_simultaneous_requests_per_host,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level,
            port=port)
        
    def start(self):
        reactor.callWhenRunning(self._start)
        return self.start_deferred

    def _start(self):
        deferreds = []
        d = DeferredList(deferreds, consumeErrors=True)
        d.addCallback(self._startCallback)

    def _startCallback(self, data):
        for row in data:
            if row[0] == False:
                d = self.shutdown()
                d.addCallback(self._startHandleError, row[1])
                return d
        d = CassandraServer.start(self)

    def shutdown(self):
        deferreds = []
        LOGGER.debug("%s stopping on main HTTP interface." % self.name)
        d = self.site_port.stopListening()
        if isinstance(d, Deferred):
            deferreds.append(d)
        if len(deferreds) > 0:
            d = DeferredList(deferreds)
            d.addCallback(self._shutdownCallback)
            return d
        else:
            return self._shutdownCallback(None)

    def _shutdownCallback(self, data):
        return CassandraServer.shutdown(self)
    
    def enqueueUUID(self, uuid):
        if uuid and self.scheduler_server is not None:
            parameters = {'uuid': uuid}
            query_string = urllib.urlencode(parameters)       
            url = 'http://%s:%s/function/schedulerserver/enqueueuuid?%s' % (self.scheduler_server, self.scheduler_server_port, query_string)
            LOGGER.info('Sending UUID to scheduler to be queued: %s' % url)
            d = self.rq.getPage(url=url)
            d.addCallback(self._enqueueCallback, uuid)
            d.addErrback(self._enqueueErrback, uuid)
            return d
        return None
    
    def _enqueueCallback(self, data, uuid):
        LOGGER.info("%s is added to spider queue." % uuid)
    
    def _enqueueErrback(self, error, uuid):
        LOGGER.info("Could not enqueue %s." % uuid)
            
