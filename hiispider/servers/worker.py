from .cassandra import CassandraServer
from .base import LOGGER, Job
from ..resources import WorkerResource
from ..amqp import amqp as AMQP
from txamqp.content import Content
from MySQLdb.cursors import DictCursor
from twisted.internet import reactor, task
from twisted.enterprise import adbapi
from twisted.web import server
from twisted.internet.defer import inlineCallbacks, returnValue
from uuid import UUID
from zlib import compress, decompress
import pprint
import simplejson
from hashlib import sha256
import cPickle 
from traceback import format_exc

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)


class WorkerServer(CassandraServer):

    public_ip = None
    local_ip = None
    network_information = {}
    simultaneous_jobs = 50
    jobs_complete = 0
    job_queue = []
    jobsloop = None
    dequeueloop = None
    queue_requests = 0
    amqp_pagecache_queue_size = 0
    # Define these in case we have an early shutdown.
    chan = None
    pagecache_chan = None
    
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
            mysql_username=None,
            mysql_password=None,
            mysql_host=None,
            mysql_database=None,
            amqp_host=None,
            amqp_username=None,
            amqp_password=None,
            amqp_vhost=None,
            amqp_pagecache_vhost=None,
            amqp_queue=None,
            amqp_exchange=None,
            redis_hosts=None,
            disable_negative_cache=False,
            scheduler_server=None,
            scheduler_server_port=5001,
            pagecache_web_server_host=None,
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
        self.cassandra_cf_temp_content = cassandra_cf_temp_content
        self.cassandra_cf_content=cassandra_cf_content
        self.cassandra_content=cassandra_content
        # Cassandra Stats
        self.cassandra_stats_keyspace=cassandra_stats_keyspace
        self.cassandra_stats_cf_daily=cassandra_stats_cf_daily
        # Redis
        self.redis_hosts = redis_hosts
        # Pagecache
        self.pagecache_web_server_host = pagecache_web_server_host
        self.amqp_pagecache_vhost = amqp_pagecache_vhost
        # Negative Cache Disabled?
        self.disable_negative_cache = disable_negative_cache
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
            cassandra_cf_temp_content=cassandra_cf_temp_content,
            cassandra_cf_content=cassandra_cf_content,
            cassandra_content=cassandra_content,
            cassandra_stats_keyspace=cassandra_stats_keyspace,
            cassandra_stats_cf_daily=cassandra_stats_cf_daily,
            redis_hosts=redis_hosts,
            disable_negative_cache=disable_negative_cache,
            max_simultaneous_requests=max_simultaneous_requests,
            max_requests_per_host_per_second=max_requests_per_host_per_second,
            max_simultaneous_requests_per_host=max_simultaneous_requests_per_host,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level)

    def start(self):
        start_deferred = super(WorkerServer, self).start()
        start_deferred.addCallback(self._workerStart)
        return start_deferred

    @inlineCallbacks
    def _workerStart(self, started):
        LOGGER.debug("Starting Worker components.")
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
        # setup pagecache queue
        LOGGER.info('Connecting to pagecache broker.')
        self.pagecache_conn = yield AMQP.createClient(
            self.amqp_host,
            self.amqp_pagecache_vhost,
            self.amqp_port)
        yield self.pagecache_conn.authenticate(self.amqp_username, self.amqp_password)
        self.pagecache_chan = yield self.pagecache_conn.channel(1)
        yield self.pagecache_chan.channel_open()
        # Create pagecache Queue
        yield self.pagecache_chan.queue_declare(
            queue=self.amqp_queue,
            durable=False,
            exclusive=False,
            auto_delete=False)
        # Create pagecache Exchange
        yield self.pagecache_chan.exchange_declare(
            exchange=self.amqp_exchange,
            type="fanout",
            durable=False,
            auto_delete=False)
        yield self.pagecache_chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
        # Setup cassandra
        LOGGER.info('Connecting to cassandra')
        self.jobsloop = task.LoopingCall(self.executeJobs)
        self.jobsloop.start(0.2)
        LOGGER.info('Starting dequeueing thread...')
        self.dequeueloop = task.LoopingCall(self.dequeue)
        self.dequeueloop.start(1)
        self.pagecachestatusloop = task.LoopingCall(self.pagecacheQueueStatusCheck)
        self.pagecachestatusloop.start(60)

    @inlineCallbacks
    def shutdown(self):
        LOGGER.debug("Closing connection")
        try:
            self.jobsloop.cancel()
            self.dequeueloop.cancel()
        except:
            pass
        # Shut things down
        LOGGER.info('Closing MYSQL Connnection Pool')
        yield self.mysql.close()
        LOGGER.info('Closing Spider Queue')
        if self.chan is not None:
            yield self.chan.channel_close()
            chan0 = yield self.conn.channel(0)
            yield chan0.connection_close()
            LOGGER.info('Closing Pagecache Queue')
        if self.pagecache_chan is not None:
            yield self.pagecache_chan.channel_close()
            pagecache_chan0 = yield self.pagecache_conn.channel(0)
            yield pagecache_chan0.connection_close()
        
    @inlineCallbacks
    def pagecacheQueueStatusCheck(self):
        yield self.pagecache_chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
        pagecache_queue_status = yield self.pagecache_chan.queue_declare(
            queue=self.amqp_queue,
            passive=True)
        self.amqp_pagecache_queue_size = pagecache_queue_status.fields[1]
        LOGGER.debug('Pagecache queue size: %d' % self.amqp_pagecache_queue_size)

    def dequeue(self):
        LOGGER.debug('Completed Jobs: %d / Queued Jobs: %d / Active Jobs: %d' % (self.jobs_complete, len(self.job_queue), len(self.active_jobs)))
        while len(self.job_queue) + self.queue_requests <= self.amqp_prefetch_count:
            self.queue_requests += 1
            LOGGER.debug('Fetching from queue, %s queue requests.' % self.queue_requests)
            self.dequeue_item()


    @inlineCallbacks
    def dequeue_item(self):
        LOGGER.debug('Getting job.')
        try:
            msg = yield self.queue.get()
        except Exception, e:
            LOGGER.error('Dequeue Error: %s' % e)
            return
        if msg.delivery_tag:
            try:
                LOGGER.debug('basic_ack for delivery_tag: %s' % msg.delivery_tag)
                yield self.chan.basic_ack(msg.delivery_tag)
            except Exception, e:
                LOGGER.error('basic_ack Error: %s' % e)
        self.queue_requests -= 1
        uuid = UUID(bytes=msg.content.body).hex
        LOGGER.debug('Got job %s' % uuid)
        try:
            job = yield self.getJob(uuid)
        except Exception, e:
            LOGGER.error('Job Error: %s\n%s' % (e, format_exc()))
            return
        if job.function_name in self.functions:
            LOGGER.debug('Successfully pulled job off of AMQP queue')
            if self.functions[job.function_name]["check_reservation_fast_cache"]:
                job.fast_cache = yield self.getFastCache(job.uuid)
            self.job_queue.append(job)
        else:
            LOGGER.error("Could not find function %s." % function_name)
            return
    
    def executeJobs(self):
        while len(self.job_queue) > 0 and len(self.active_jobs) < self.simultaneous_jobs:
            job = self.job_queue.pop(0)
            d = self.executeJob(job)
            d.addCallback(self.storeInPagecache, job)
            
    @inlineCallbacks
    def executeJob(self, job):
        data = yield self.callExposedFunction(
            self.functions[job.function_name]["function"],
            job.kwargs,
            job.function_name,
            reservation_fast_cache=job.fast_cache,
            uuid=job.uuid)
        self.jobs_complete += 1
        LOGGER.debug('Completed Jobs: %d / Queued Jobs: %d / Active Jobs: %d' % (
            self.jobs_complete, 
            len(self.job_queue), 
            len(self.active_jobs)))
        returnValue(data)

    def storeInPagecache(self, data, job):
        if not self.amqp_pagecache_vhost:
            return
        if self.amqp_pagecache_queue_size > 100000:
            LOGGER.error('Pagecache Queue Size has exceeded 100,000 items')
            return
        pagecache_msg = {}
        if "host" in job.user_account and job.user_account['host']:
            cache_key = job.user_account['host']
            pagecache_msg['host'] = job.user_account['host']
        else:
            cache_key = '%s/%s' % (self.pagecache_web_server_host, job.user_account['username'])
        pagecache_msg['username'] = job.user_account['username']
        pagecache_msg['cache_key'] = sha256(cache_key).hexdigest()
        msg = Content(simplejson.dumps(pagecache_msg))
        d = self.pagecache_chan.basic_publish(exchange=self.amqp_exchange, content=msg)
        d.addErrback(self._pagecacheErrback)
        return d
        
    def _pagecacheErrback(self, error):
        LOGGER.error('Pagecache Error: %s' % str(error))

    def workerErrback(self, error, function_name='Worker', delivery_tag=None):
        LOGGER.error('%s Error: %s' % (function_name, str(error)))
        LOGGER.debug('Queued Jobs: %d / Active Jobs: %d' % (len(self.job_queue), len(self.active_jobs)))
        LOGGER.debug('Active Jobs List: %s' % repr(self.active_jobs))
        return error

    def getJob(self, uuid):
        d = self.redis_client.get(uuid)
        d.addCallback(self._getJobCallback, uuid)
        d.addErrback(self._getJobErrback, uuid)
        return d

    def _getJobCallback(self, account, uuid, delivery_tag):
        if account:
            job = cPickle.loads(decompress(account))
            LOGGER.debug('Found uuid in redis: %s' % uuid)
            return job
        else:
            d = self._getJobErrback(None, uuid)
            return d
            
    @inlineCallbacks
    def _getJobErrback(self, error, uuid):
        LOGGER.debug('Could not find uuid in redis: %s' % uuid)
        user_account = yield self.getUserAccount(uuid)
        service_type = user_account['type'].split('/')[0].lower()
        account_id = user_account['account_id']
        service_credentials = yield self.getServiceCredentials(service_type, account_id)
        job = Job(
            function_name=user_account['type'],
            uuid=uuid,
            service_credentials=service_credentials,
            user_account=user_account,
            functions=self.functions,
            service_mapping=self.service_mapping,
            service_args_mapping=self.service_args_mapping)
        self.setJobCache(job)
        returnValue(job)
        
    def getUserAccount(self, uuid):
        sql = """SELECT content_userprofile.user_id as user_id, username, host, account_id, type
            FROM spider_service, auth_user, content_userprofile
            WHERE uuid = '%s'
            AND auth_user.id=spider_service.user_id
            AND auth_user.id=content_userprofile.user_id
        """ % uuid
        d = self.mysql.runQuery(sql)
        d.addCallback(self._getUserAccountCallback, uuid)
        d.addErrback(self._getUserAccountErrback, uuid)
        return d

    def _getUserAccountCallback(self, data, uuid):
        if len(data) == 0: # No results?
            message = "Could not find user %s: %s" % uuid
            LOGGER.error(message)
            raise Exception(message)
        return data[0]
        
    def _getUserAccountErrback(self, error, uuid):
        LOGGER.error("Could not get user %s: %s" % (uuid, str(error)))
        return error
        
    def getServiceCredentials(self, service_type, account_id):
        sql = "SELECT * FROM content_%saccount WHERE account_id = %d" % (service_type, account_id)
        d = self.mysql.runQuery(sql)
        d.addCallback(self._getServiceCredentialsCallback, service_type, account_id)
        d.addErrback(self._getAccountErrback, service_type, account_id)
        return d

    def _getServiceCredentialsCallback(self, data, service_type, account_id):
        if len(data) == 0: # No results?
            message = "Could not find service %s:%s" % (service_type, account_id)
            LOGGER.error(message)
            raise Exception(message)
        return data[0]

    def _getAccountErrback(self, error, account_type, account_id):
        LOGGER.error("Could not find account data %s:%s - %s" % (account_type, account_id, str(error)))
        return error

    def getFastCache(self, uuid):
        d = self.redis_client.get("fastcache:%s" % uuid)
        d.addErrback(self._getFastCacheErrback, uuid)
        return d

    def _getFastCacheErrback(self, error, uuid):
        LOGGER.debug("Could not get Fast Cache for %s" % uuid)
        return None

    def setFastCache(self, uuid, data):
        if not isinstance(data, str):
            raise Exception("FastCache must be a string.")
        if uuid is None:
            return None
        d = self.redis_client.set("fastcache:%s" % uuid, data)
        d.addCallback(self._setFastCacheCallback, uuid)
        d.addErrback(self._setFastCacheErrback)

    def _setFastCacheCallback(self, data, uuid):
        LOGGER.debug("Successfully set Fast Cache for %s" % uuid)

    def _setFastCacheErrback(self, error):
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
        return cPickle.loads(decompress(data))
        
    @inlineCallbacks
    def setJobCache(self, job):
        """Set job cache in redis. Expires at now + 7 days."""
        job_data = compress(cPickle.dumps(job), 1)
        # TODO: Figure out why txredisapi thinks setex doesn't like sharding.
        try:
            yield self.redis_client.set(job.uuid, job_data)
            yield self.redis_client.expire(job.uuid, 60*60*24*7)
        except Exception, e:
            LOGGER.error(str(e))
            
