import pprint
import urllib
from uuid import uuid4
from MySQLdb.cursors import DictCursor
from twisted.enterprise import adbapi
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred, inlineCallbacks
from twisted.internet.threads import deferToThread
from twisted.web.resource import Resource
from twisted.internet import reactor
from twisted.web import server
from .cassandra import CassandraServer
from .base import LOGGER, SmartConnectionPool
from ..resources import InterfaceResource
from ..evaluateboolean import evaluateBoolean
import zlib
from ..amqp import amqp as AMQP


PRETTYPRINTER = pprint.PrettyPrinter(indent=4)

class InterfaceServer(CassandraServer):
    
    name = "HiiSpider Interface Server UUID: %s" % str(uuid4())
    
    def __init__(self,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            cassandra_server=None,
            cassandra_keyspace=None,
            cassandra_stats_keyspace=None,
            cassandra_stats_cf_daily=None,
            cassandra_cf_temp_content=None,
            cassandra_cf_content=None,
            cassandra_content=None,
            redis_hosts=None,
            amqp_host=None,
            amqp_username=None,
            amqp_password=None,
            amqp_vhost=None,
            amqp_queue=None,
            amqp_exchange=None,
            amqp_port=5672,
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
        # AMQP connection parameters
        self.amqp_host = amqp_host
        self.amqp_vhost = amqp_vhost
        self.amqp_port = amqp_port
        self.amqp_username = amqp_username
        self.amqp_password = amqp_password
        self.amqp_queue = amqp_queue
        self.amqp_exchange = amqp_exchange
        # Cassandra
        self.cassandra_server=cassandra_server
        self.cassandra_keyspace=cassandra_keyspace
        self.cassandra_cf_temp_content = cassandra_cf_temp_content
        self.cassandra_cf_content=cassandra_cf_content
        self.cassandra_content=cassandra_content
        # Cassandra Stats
        self.cassandra_stats_keyspace=cassandra_stats_keyspace
        self.cassandra_stats_cf_daily=cassandra_stats_cf_daily
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
            cassandra_stats_keyspace=cassandra_stats_keyspace,
            cassandra_stats_cf_daily=cassandra_stats_cf_daily,
            cassandra_cf_temp_content=cassandra_cf_temp_content,
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
        start_deferred = super(InterfaceServer, self).start()
        start_deferred.addCallback(self._interfaceStart)
        return start_deferred
    
    @inlineCallbacks
    def _interfaceStart(self):
        LOGGER.info('Connecting to broker.')
        self.conn = yield AMQP.createClient(
            self.amqp_host,
            self.amqp_vhost,
            self.amqp_port)
        yield self.conn.authenticate(self.amqp_username, self.amqp_password)
        self.chan = yield self.conn.channel(1)
        yield self.chan.channel_open()
        # Create Queue
        yield self.chan.queue_declare(
            queue=self.amqp_queue,
            durable=False,
            exclusive=False,
            auto_delete=False)
        # Create Exchange
        yield self.chan.exchange_declare(
            exchange=self.amqp_exchange,
            type="fanout",
            durable=False,
            auto_delete=False)
        yield self.chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
    
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
            
