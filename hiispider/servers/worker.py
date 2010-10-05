from .cassandra import CassandraServer
from .base import LOGGER
from ..resources import WorkerResource
from ..networkaddress import getNetworkAddress
from ..amqp import amqp as AMQP
from ..resources import InterfaceResource, ExposedResource
from MySQLdb.cursors import DictCursor
from twisted.internet import reactor, protocol, task
from twisted.enterprise import adbapi
from twisted.web import server
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred, inlineCallbacks
from twisted.internet.threads import deferToThread
from uuid import UUID, uuid4
from zlib import compress, decompress
import cjson
import pprint
import simplejson

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)

class WorkerServer(CassandraServer):
    
    public_ip = None
    local_ip = None
    network_information = {}
    simultaneous_jobs = 50
    jobs_complete = 0
    job_queue = []
    job_queue_a = job_queue.append
    jobsloop = None
    dequeueloop = None
    queue_requests = 0

    def __init__(self,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            cassandra_server=None, 
            cassandra_keyspace=None,
            cassandra_cf_cache=None,
            cassandra_cf_content=None,
            cassandra_content=None,
            cassandra_http=None,
            cassandra_headers=None,
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
            redis_hosts=None,
            scheduler_server=None,
            scheduler_server_port=5001,
            service_mapping=None,
            service_args_mapping=None,
            amqp_port=5672,
            amqp_prefetch_count=200,
            mysql_port=3306,
            max_simultaneous_requests=100,
            max_requests_per_host_per_second=0,
            max_simultaneous_requests_per_host=0,
            port=6000,
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
        # Cassandra
        self.cassandra_server=cassandra_server
        self.cassandra_keyspace=cassandra_keyspace
        self.cassandra_cf_cache=cassandra_cf_cache
        self.cassandra_cf_content=cassandra_cf_content
        self.cassandra_http=cassandra_http
        self.cassandra_headers=cassandra_headers
        self.cassandra_content=cassandra_content
        # Redis
        self.redis_hosts = redis_hosts
        # Resource Mappings
        self.service_mapping = service_mapping
        self.service_args_mapping = service_args_mapping
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
        CassandraServer.__init__(
            self,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            cassandra_server=cassandra_server,
            cassandra_keyspace=cassandra_keyspace,
            cassandra_cf_cache=cassandra_cf_cache,
            cassandra_cf_content=cassandra_cf_content,
            cassandra_content=cassandra_content,
            cassandra_http=cassandra_http,
            cassandra_headers=cassandra_headers,
            redis_hosts=redis_hosts,
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
        LOGGER.info('Connecting to broker.')
        self.conn = yield AMQP.createClient(
            self.amqp_host,
            self.amqp_vhost,
            self.amqp_port)
        self.auth = yield self.conn.authenticate(
            self.amqp_username,
            self.amqp_password)
        self.chan = yield self.conn.channel(2)
        yield self.chan.channel_open()
        yield self.chan.basic_qos(prefetch_count=self.amqp_prefetch_count)
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
        yield self.chan.basic_consume(queue=self.amqp_queue,
            no_ack=False,
            consumer_tag="hiispider_consumer")
        self.queue = yield self.conn.queue("hiispider_consumer")
        yield CassandraServer.start(self)
        self.jobsloop = task.LoopingCall(self.executeJobs)
        self.jobsloop.start(0.2)
        LOGGER.info('Starting dequeueing thread...')
        self.dequeueloop = task.LoopingCall(self.dequeue)
        self.dequeueloop.start(0.2)
    
    @inlineCallbacks
    def shutdown(self):
        LOGGER.debug("Closing connection")
        try:
            self.jobsloop.cancel()
        except:
            pass
        # Shut things down
        LOGGER.info('Closing MYSQL Connnection Pool')
        yield self.mysql.close()
        yield self.chan.channel_close()
        chan0 = yield self.conn.channel(0)
        yield chan0.connection_close()
                
    def dequeue(self):
        LOGGER.debug('Completed Jobs: %d / Queued Jobs: %d / Active Jobs: %d' % (self.jobs_complete, len(self.job_queue), len(self.active_jobs)))
        while len(self.job_queue) + self.queue_requests <= self.amqp_prefetch_count:
            self.queue_requests += 1
            LOGGER.debug('Fetching from queue')
            d = self.queue.get()
            d.addCallback(self._dequeueCallback)
            d.addErrback(self._dequeueErrback)
            
    def _dequeueErrback(self, error):
        LOGGER.error('Dequeue Error: %s' % error)
        self.queue_requests -= 1
        
    def _dequeueCallback(self, msg):
        self.queue_requests -= 1
        if msg.delivery_tag:
            LOGGER.debug('basic_ack for delivery_tag: %s' % msg.delivery_tag)
            d = self.chan.basic_ack(msg.delivery_tag)
            d.addCallback(self._dequeueCallback2, msg)
            d.addErrback(self.basicAckErrback)
            return d
        else:
            self._dequeueCallback2(data=True, msg=msg)
        
    def _dequeueCallback2(self, data, msg):
        LOGGER.debug('fetched msg from queue: %s' % repr(msg))
        # Get the hex version of the UUID from byte string we were sent
        uuid = UUID(bytes=msg.content.body).hex
        d = self.getJob(uuid, msg.delivery_tag)
        d.addCallback(self._dequeueCallback3, msg)
        d.addErrback(self._dequeueErrback)
    
    def _dequeueCallback3(self, job, msg):
        # Load custom function.
        if job is not None:
            if job['function_name'] in self.functions:
                LOGGER.debug('Successfully pulled job off of AMQP queue')
                job['exposed_function'] = self.functions[job['function_name']]
                if not job.has_key('kwargs'):
                    job['kwargs'] = self.mapKwargs(job)
                if not job.has_key('delivery_tag'):
                    job['delivery_tag'] = msg.delivery_tag
                # If function asked for fast_cache, try to fetch it from redis
                # while it's queued. Go ahead and add it to the queue in the meantime
                # to speed things up.
                job["reservation_fast_cache"] = None
                if self.functions[job['function_name']]["check_reservation_fast_cache"]:
                    d = self.getReservationFastCache(job['uuid'])
                    d.addCallback(self._dequeueCallback4, job)
                self.job_queue_a(job)
            else:
                LOGGER.error("Could not find function %s." % job['function_name'])
    
    def _dequeueCallback4(self, data, job):
        job["reservation_fast_cache"] = data
    
    def executeJobs(self):
        while len(self.job_queue) > 0 and len(self.active_jobs) < self.simultaneous_jobs:
            job = self.job_queue.pop(0)
            exposed_function = job["exposed_function"]
            kwargs = job["kwargs"]
            function_name = job["function_name"]
            if job.has_key('uuid'):
                uuid = job["uuid"]
            else:
                # assign a temp uuid
                uuid = UUID(bytes=msg.content.body).hex
            d = self.callExposedFunction(
                exposed_function["function"], 
                kwargs, 
                function_name,
                reservation_fast_cache=job["reservation_fast_cache"],
                uuid=uuid)
            d.addCallback(self._executeJobCallback, job)
            d.addErrback(self.workerErrback, 'Execute Jobs', job['delivery_tag'])
        
    def _executeJobCallback(self, data, job):
        self.jobs_complete += 1
        LOGGER.debug('Completed Jobs: %d / Queued Jobs: %d / Active Jobs: %d' % (self.jobs_complete, len(self.job_queue), len(self.active_jobs)))
        if job.has_key('exposed_function'):
            del(job['exposed_function'])
        # Only cache this when the cache is empty
        d = self.getJobCache(job['uuid'])
        d.addCallback(self._executeJobCallback2, job)
        d.addErrback(self.workerErrback, 'Execute Jobs', job['delivery_tag'])
        return d
        
    def _executeJobCallback2(self, data, job):
        if data is None:
            d = self.setJobCache(job)
            d.addCallback(self._executeJobCallback3)
            d.addErrback(self.workerErrback, 'Execute Jobs', job['delivery_tag'])
            return d
        else:
            return None

    def _executeJobCallback3(self, data):
        return
        
    def workerErrback(self, error, function_name='Worker', delivery_tag=None):
        LOGGER.error('%s Error: %s' % (function_name, str(error)))
        LOGGER.debug('Queued Jobs: %d / Active Jobs: %d' % (len(self.job_queue), len(self.active_jobs)))
        LOGGER.debug('Active Jobs List: %s' % repr(self.active_jobs))
        return error
        
    def _basicAckCallback(self, data):
        return
        
    def basicAckErrback(self, error):
        LOGGER.error('basic_ack Error: %s' % (error))
        return
        
    def getJob(self, uuid, delivery_tag):
        d = self.redis_client.get(uuid)
        d.addCallback(self._getJobCallback, uuid, delivery_tag)
        d.addErrback(self._getJobErrback, uuid, delivery_tag)
        return d
    
    def _getJobErrback(self, account, uuid, delivery_tag):
        LOGGER.debug('Could not find uuid in redis: %s' % uuid)
        sql = "SELECT account_id, type FROM spider_service WHERE uuid = '%s'" % uuid
        d = self.mysql.runQuery(sql)
        d.addCallback(self.getAccountMySQL, uuid, delivery_tag)
        d.addErrback(self.workerErrback, uuid, delivery_tag)
        return d
        
    def _getJobCallback(self, account, uuid, delivery_tag):
        job = cjson.decode(decompress(account))
        LOGGER.debug('Found uuid in redis: %s' % uuid)
        return job
    
    def getAccountMySQL(self, spider_info, uuid, delivery_tag):
        if spider_info:
            account_type = spider_info[0]['type'].split('/')[0]
            sql = "SELECT * FROM content_%saccount WHERE account_id = %d" % (account_type.lower(), spider_info[0]['account_id'])
            d = self.mysql.runQuery(sql)
            d.addCallback(self.createJob, spider_info, uuid, delivery_tag)
            d.addErrback(self.workerErrback, 'Get MySQL Account', delivery_tag)
            return d
        LOGGER.debug('No spider_info given for uuid %s' % uuid)
        return None
    
    def createJob(self, account_info, spider_info, uuid, delivery_tag):
        job = {}
        account = account_info[0]
        function_name = spider_info[0]['type']
        job['type'] = function_name.split('/')[1]
        if self.service_mapping and self.service_mapping.has_key(function_name):
            LOGGER.debug('Remapping resource %s to %s' % (function_name, self.service_mapping[function_name]))
            function_name = self.service_mapping[function_name]
        job['function_name'] = function_name
        job['uuid'] = uuid
        job['account'] = account
        job['delivery_tag'] = delivery_tag
        return job
    
    def mapKwargs(self, job):
        kwargs = {}
        service_name = job['function_name'].split('/')[0]
        # remap some fields that differ from the plugin and the database
        if service_name in self.service_args_mapping:
            for key in self.service_args_mapping[service_name]:
                if key in job['account']:
                    job['account'][self.service_args_mapping[service_name][key]] = job['account'][key]
        # apply job fields to req and optional kwargs
        for arg in job['exposed_function']['required_arguments']:
            if arg in job:
                kwargs[str(arg)] = job[arg]
            elif arg in job['account']:
                kwargs[str(arg)] = job['account'][arg]
        for arg in job['exposed_function']['optional_arguments']:
            if arg in job['account']:
                kwargs[str(arg)] = job['account'][arg]
        LOGGER.debug('Function: %s\nKWARGS: %s' % (job['function_name'], repr(kwargs)))
        return kwargs
        
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
    
    def getReservationFastCache(self, uuid):
        d = self.redis_client.get("fastcache:%s" % uuid)
        d.addCallback(self._getReservationFastCacheCallback, uuid)
        return d
        
    def _getReservationFastCacheCallback(self, value, uuid):
        if value:
            LOGGER.debug("Successfully got Fast Cache for %s" % uuid)
            return value
        else:
            LOGGER.debug("Could not get Fast Cache for %s" % uuid)
            return None
        
    def setReservationFastCache(self, uuid, data):
        if not isinstance(data, str):
            raise Exception("ReservationFastCache must be a string.")
        if uuid is None:
            return None
        d = self.redis_client.set("fastcache:%s" % uuid, data)
        d.addCallback(self._setReservationFastCacheCallback, uuid)
        d.addErrback(self._setReservationFastCacheErrback)
    
    def _setReservationFastCacheCallback(self, data, uuid):
        LOGGER.debug("Successfully set Fast Cache for %s" % uuid)
        
    def _setReservationFastCacheErrback(self, error):
        LOGGER.error(str(error))
        
    def getJobCache(self, uuid):
        """Search for job info in redis cache. Returns None if not found."""
        d = self.redis_client.get(uuid)
        d.addCallback(self._getJobCacheCallback)
        d.addErrback(self._getJobCacheErrback)
        return d

    def _getJobCacheErrback(self, error):
        return None

    def _getJobCacheCallback(self, data):
        return cjson.decode(decompress(data))

    def setJobCache(self, job):
        """Set job cache in redis. Expires at now + 7 days."""
        # TODO: Figure out why txredisapi thinks setex doesn't like sharding.
        d = self.redis_client.set(job['uuid'], compress(cjson.encode(job)))
        d.addCallback(self._setJobCacheCallback, job)
        d.addErrback(self.workerErrback, 'Execute Jobs', job['delivery_tag'])
        return d
        
    def _setJobCacheCallback(self, data, job):
        d = self.redis_client.expire(job['uuid'], 60*60*24*7)
        d.addCallback(self._setJobCacheCallback2)
        d.addErrback(self.workerErrback, 'Execute Jobs', job['delivery_tag'])
        return d

    def _setJobCacheCallback2(self, data):
        return

