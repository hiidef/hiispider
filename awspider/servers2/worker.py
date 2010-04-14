from .base import BaseServer, LOGGER
from ..resources2 import WorkerResource
from ..networkaddress import getNetworkAddress
from ..amqp import amqp as AMQP
from ..resources import InterfaceResource, ExposedResource
from MySQLdb.cursors import DictCursor
from twisted.internet import reactor, protocol
from twisted.enterprise import adbapi
from twisted.web import server
from twisted.protocols.memcache import MemCacheProtocol, DEFAULT_PORT
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred, inlineCallbacks
from twisted.internet.threads import deferToThread
from uuid import UUID, uuid4
import pprint
import cPickle as pickle

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)

class WorkerServer(BaseServer):
    
    public_ip = None
    local_ip = None
    network_information = {}
    simultaneous_jobs = 50
    doSomethingCallLater = None
    
    def __init__(self,
            aws_access_key_id,
            aws_secret_access_key,
            aws_s3_http_cache_bucket=None,
            aws_s3_storage_bucket=None,
            mysql_username=None,
            mysql_password=None,
            mysql_host=None,
            mysql_database=None,
            amqp_host=None,
            amqp_username=None,
            amqp_password=None,
            amqp_vhost=None,
            amqp_queue=None,
            amqp_exchange=None,
            memcached_host=None,
            resource_mapping=None,
            amqp_port=5672,
            amqp_prefetch_count=100,
            mysql_port=3306,
            memcached_port=11211,
            max_simultaneous_requests=100,
            max_requests_per_host_per_second=0,
            max_simultaneous_requests_per_host=0,
            port=5005,
            log_file='workerserver.log',
            log_directory=None,
            log_level="debug"):
        self.network_information["port"] = port
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
        # Create Memcached client
        self.memcached_host = memcached_host
        self.memcached_port = memcached_port
        self.memc_ClientCreator = protocol.ClientCreator(
            reactor, MemCacheProtocol)
        # Resource Mappings
        self.resource_mapping = resource_mapping
        # HTTP interface
        resource = WorkerResource(self)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Create AMQP Connection
        # AMQP connection parameters
        self.amqp_host = amqp_host
        self.amqp_vhost = amqp_vhost
        self.amqp_port = amqp_port
        self.amqp_username = amqp_username
        self.amqp_password = amqp_password
        self.amqp_queue = amqp_queue
        self.amqp_exchange = amqp_exchange
        self.amqp_prefetch_count = amqp_prefetch_count
        BaseServer.__init__(
            self,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_s3_http_cache_bucket=aws_s3_http_cache_bucket,
            aws_s3_storage_bucket=aws_s3_storage_bucket,
            max_simultaneous_requests=max_simultaneous_requests,
            max_requests_per_host_per_second=max_requests_per_host_per_second,
            max_simultaneous_requests_per_host=max_simultaneous_requests_per_host,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level)
    
    def start(self):
        reactor.callWhenRunning(self._start)
        return self.start_deferred
    
    @inlineCallbacks
    def _start(self):
        yield self.getNetworkAddress()
        # Create memcached client
        self.memc = yield self.memc_ClientCreator.connectTCP(self.memcached_host, self.memcached_port)
        LOGGER.info('Connecting to broker.')
        self.conn = yield AMQP.createClient(
            self.amqp_host,
            self.amqp_vhost,
            self.amqp_port)
        self.auth = yield self.conn.authenticate(
            self.amqp_username,
            self.amqp_password)
        self.chan = yield self.conn.channel(1)
        yield self.chan.channel_open()
        yield self.chan.basic_qos(prefetch_count=self.amqp_prefetch_count)
        # Create Queue
        yield self.chan.queue_declare(
            queue=self.amqp_queue,
            durable=False,
            exclusive=False,
            auto_delete=False)
        yield self.chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
        d = self.chan.basic_consume(queue=self.amqp_queue,
            no_ack=False,
            consumer_tag="awspider_consumer")
        d.addCallback(self.dequeue)
        self.queue = yield self.conn.queue("awspider_consumer")
        yield BaseServer.start(self)
    
    @inlineCallbacks
    def shutdown(self):
        LOGGER.debug("Closting connection")
        try:
            self.doSomethingCallLater.cancel()
        except:
            pass
        # Shut things down
        LOGGER.info('Closing broker connection')
        yield self.chan.channel_close()
        chan0 = yield self.conn.channel(0)
        yield chan0.connection_close()
    
    def dequeue(self, consumer=None):
        if len(self.active_jobs) < self.simultaneous_jobs:
            deferToThread(self._dequeue)
        else:
            LOGGER.info('Maximum simultaneous jobs running (%d/%d)' % (self.active_jobs, self.simultaneous_jobs))
    
    def _dequeue(self):
        d = self.queue.get()
        d.addCallback(self._dequeue2)
        d.addErrback(self.workerError)
    
    def _dequeue2(self, msg):
        if msg:
            # Get the hex version of the UUID from byte string we were sent
            uuid = UUID(bytes=msg.content.body).hex
            d = self.getJob(uuid)
            d.addCallback(self._dequeue3, msg)
            d.addErrback(self.workerError)
    
    def _dequeue3(self, job, msg):
        if job:
            # Load custom function.
            if job['function_name'] in self.functions:
                job['exposed_function'] = self.functions[job['function_name']]
            else:
                LOGGER.error("Could not find function %s." % job['function_name'])
            LOGGER.info('Pulled job off of AMQP queue')
            job['kwargs'] = self.mapKwargs(job)
            d = self.chan.basic_ack(delivery_tag=msg.delivery_tag)
            d.addCallback(self.executeJob, job)
            d.addErrback(self.workerError)
        else:
            self.dequeueCallLater = reactor.callLater(1, self.dequeue)
    
    def executeJob(self, ack, job):
        exposed_function = job["exposed_function"]
        kwargs = job["kwargs"]
        function_name = job["function_name"]
        uuid = job["uuid"]
        d = self.callExposedFunction(
            exposed_function["function"], 
            kwargs, 
            function_name, 
            uuid=uuid)
        d.addCallback(self._executeJob)
        d.addErrback(self.workerError)
        
    def _executeJob(self, data):
        self.dequeueCallLater = reactor.callLater(1, self.dequeue)
        
    def workerError(self, error):
        LOGGER.error(error)
        raise error
    
    def getJob(self, uuid):
        d = self.memc.get(uuid)
        d.addCallback(self._getJob, uuid)
        d.addErrback(self.workerError)
        return d
    
    def _getJob(self, account, uuid):
        job = account[1]
        if not job:
            LOGGER.debug('Could not find uuid in memcached: %s' % uuid)
            sql = "SELECT account_id, type FROM spider_service WHERE uuid = '%s'" % uuid
            d = self.mysql.runQuery(sql)
            d.addCallback(self._getAccountMySQL, uuid)
            d.addErrback(self.workerError)
            return d
        else:
            LOGGER.debug('Found uuid in memcached: %s' % uuid)
            return pickle.loads(job)
    
    def _getAccountMySQL(self, spider_info, uuid):
        if spider_info:
            try:
                LOGGER.debug(spider_info[0]['type'])
                account_type = spider_info[0]['type'].split('/')[0]
                sql = "SELECT * FROM content_%saccount WHERE account_id = %d" % (account_type, spider_info[0]['account_id'])
                d = self.mysql.runQuery(sql)
                d.addCallback(self.createJob, spider_info, uuid)
                d.addErrback(self.workerError)
                return d
            except:
                LOGGER.error(spider_info)
                raise
        LOGGER.critical('No spider_info given for uuid %s' % uuid)
        return None
    
    def createJob(self, account_info, spider_info, uuid):
        job = {}
        account = account_info[0]
        function_name = spider_info[0]['type']
        if self.resource_mapping and self.resource_mapping.has_key(function_name):
            LOGGER.info('Remapping resource %s to %s' % (function_name, self.resource_mapping[function_name]))
            function_name = self.resource_mapping[function_name]
        job['function_name'] = function_name
        job['reservation_cache'] = None
        job['uuid'] = uuid
        job['account'] = account
        # Save account info in memcached for up to 7 days
        d = self.memc.set(uuid, pickle.dumps(job), 60*60*24*7)
        d.addCallback(self._createJob, job)
        d.addErrback(self.workerError)
        return d
    
    def _createJob(self, memc, job):
        return job
        
    def mapKwargs(self, job):
        kwargs = {}
        service_name = job['function_name'].split('/')[0]
        # remap some basic fields that differ from the plugin and the database
        if ('%s_user_id' % service_name) in job['account']:
            job['account']['user_id'] = job['account']['%s_user_id' % service_name]
            job['account']['username'] = job['account']['%s_user_id' % service_name]
        if 'session_key' in job['account']:
            job['account']['sk'] = job['account']['session_key']
        if 'secret' in job['account']:
            job['account']['token_secret'] = job['account']['secret']
        if 'key' in job['account']:
            job['account']['token_key'] = job['account']['key']
        for arg in job['exposed_function']['required_arguments']:
            if arg in job['account']:
                kwargs[arg] = job['account'][arg]
        for arg in job['exposed_function']['optional_arguments']:
            if arg in job['account']:
                kwargs[arg] = job['account'][arg]
        return kwargs
        
    def createReservation(self, function_name, **kwargs):
        uuid = uuid4().hex
        if not isinstance(function_name, str):
            for key in self.functions:
                if self.functions[key]["function"] == function_name:
                    function_name = key
                    break
        if function_name not in self.functions:
            raise Exception("Function %s does not exist." % function_name)
        d = self.callExposedFunction(
            self.functions[function_name]["function"], 
            kwargs, 
            function_name, 
            uuid=uuid)
        d.addCallback(self._createReservationCallback, function_name, uuid)
        d.addErrback(self._createReservationErrback, function_name, uuid)
        return d
        
    def _createReservationCallback(self, data, function_name, uuid):
        return data
            
    def _createReservationErrback(self, error, function_name, uuid):
        LOGGER.error("Unable to create reservation for %s:%s, %s.\n" % (function_name, uuid, error))
        return error
        
    def getNetworkAddress(self):
        d = getNetworkAddress()
        d.addCallback(self._getNetworkAddressCallback)
        d.addErrback(self._getNetworkAddressErrback)
        return d
    
    def _getNetworkAddressCallback(self, data):
        if "public_ip" in data:
            self.public_ip = data["public_ip"]
            self.network_information["public_ip"] = self.public_ip
        if "local_ip" in data:
            self.local_ip = data["local_ip"]
            self.network_information["local_ip"] = self.local_ip
    
    def _getNetworkAddressErrback(self, error):
        message = "Could not get network address."
        LOGGER.error(message)
        raise Exception(message)