import logging
import time
from collections import defaultdict
from .cassandra import CassandraServer
from ..resources import WorkerResource
from hiispider.servers.mixins import JobQueueMixin, PageCacheQueueMixin, JobGetterMixin, JobHistoryMixin
from hiispider.requestqueuer import QueueTimeoutException
from twisted.internet import reactor, task
from twisted.web import server
from twisted.internet.defer import inlineCallbacks, returnValue
import pprint
from traceback import format_exc
from hiispider.exceptions import *
from uuid import UUID
import cPickle
from zlib import decompress

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)
logger = logging.getLogger(__name__)


class WorkerServer(CassandraServer, JobQueueMixin, PageCacheQueueMixin, JobGetterMixin, JobHistoryMixin):

    public_ip = None
    local_ip = None
    network_information = {}
    simultaneous_reqs = 50
    uuids_dequeued = 0
    jobs_complete = 0
    job_failures = 0
    job_queue = []
    jobsloop = None
    dequeueloop = None
    logloop = None
    pending_uuid_reqs = 0
    uuid_queue = []
    uuid_dequeueing = False
    uuid_queue_size = 1000
    job_queue_size = 1000
    uncached_uuid_queue = []
    uncached_uuid_dequeueing = False
    user_account_queue = []
    user_account_dequeueing = False
    service_credential_dequeueing = False

    def __init__(self, config, port=None):
        super(WorkerServer, self).__init__(config)
        self.t0 = time.time()
        self.setupJobQueue(config)
        self.setupPageCacheQueue(config)
        self.setupJobGetter(config)
        self.setupJobHistory(config)
        # HTTP interface
        resource = WorkerResource(self)
        if port is None:
            port = config["worker_server_port"]
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        self.scheduler_server = config["scheduler_server"]
        self.scheduler_server_port = config["scheduler_server_port"]
        self.config = config
        # setup manhole
        manhole_namespace = {
            'service': self,
            'globals': globals(),
        }
        reactor.listenTCP(config["manhole_worker_port"], self.getManholeFactory(manhole_namespace, admin=config["manhole_password"]))

    def start(self):
        start_deferred = super(WorkerServer, self).start()
        start_deferred.addCallback(self._workerStart)
        return start_deferred

    @inlineCallbacks
    def _workerStart(self, started=None):
        logger.debug("Starting worker components.")
        yield self.startJobQueue()
        yield self.startPageCacheQueue()
        yield self.setupJobHistory(self.config)
        self.jobsloop = task.LoopingCall(self.executeJobs)
        self.jobsloop.start(0.2)
        self.dequeueloop = task.LoopingCall(self.dequeue)
        self.dequeueloop.start(5)
        self.logloop = task.LoopingCall(self.logStatus)
        self.logloop.start(5)

    @inlineCallbacks
    def shutdown(self):
        self.uuid_queue = []
        self.uncached_uuid_queue = []
        self.user_account_queue = []
        self.jobsloop.stop()
        self.dequeueloop.stop()
        self.logloop.stop()
        yield self.stopJobQueue()
        yield super(WorkerServer, self).shutdown()
        yield self.stopPageCacheQueue()    

    def dequeue(self):
        self.dequeuejobs()
        if self.uncached_uuid_queue:
            self.lookupjobs()

    def dequeuejobs(self):
        if self.uuid_dequeueing:
            return
        self.uuid_dequeueing = True
        if len(self.job_queue) > self.job_queue_size or len(self.uuid_queue) > self.uuid_queue_size:
            return
        self._dequeuejobs()

    def _dequeuejobs(self):
        if len(self.uuid_queue) < self.uuid_queue_size * 2:
            d = self.jobs_rabbit_queue.get()
            d.addCallback(self._dequeuejobsCallback)
            d.addErrback(self._dequeuejobsErrback)
        else:
            self.uuid_dequeueing = False

    def _dequeuejobsCallback(self, msg):
        self.jobs_chan.basic_ack(msg.delivery_tag)
        self.uuid_queue.append(UUID(bytes=msg.content.body).hex)
        self.uuids_dequeued += 1
        if len(self.uuid_queue) > 100:
            uuids, self.uuid_queue = self.uuid_queue, []
            d = self.redis_client.mget(*uuids)
            d.addCallback(self._dequeuejobsCallback2, uuids)
            d.addErrback(self._dequeuejobsErrback)
        else:
            self._dequeuejobs()

    def _dequeuejobsErrback(self, error):
        self._dequeuejobs()

    def _dequeuejobsCallback2(self, data, uuids):
        results = zip(uuids, data)
        for row in results:
            if row[1]:
                job = cPickle.loads(decompress(row[1]))
                logger.debug('Found uuid in Redis: %s' % row[0])
                self.job_queue.append(job)
            else:
                logger.debug('Could not find uuids %s in Redis.' % row[0])
                self.uncached_uuid_queue.append(row[0])
        self._dequeuejobs()

    @inlineCallbacks
    def lookupjobs(self):
        while self.uncached_uuid_queue:
            uuids, self.uncached_uuid_queue = self.uncached_uuid_queue[0:100], self.uncached_uuid_queue[100:]
            sql = """SELECT uuid, content_userprofile.user_id as user_id, username, host, account_id, type
                FROM spider_service, auth_user, content_userprofile
                WHERE uuid IN ('%s')
                AND auth_user.id=spider_service.user_id
                AND auth_user.id=content_userprofile.user_id
                """ % "','".join(uuids)
            try:
                data = yield self.mysql.runQuery(sql)
            except:
                logger.error("Could not find users in '%s'" % "','".join(uuids))
                continue
            self.user_account_queue.extend(data)
        accounts_by_type = defaultdict(list)
        for user_account in self.user_account_queue:
            accounts_by_type[user_account["type"]].append(user_account)
        self.user_account_queue = []
        for service_type in accounts_by_type:
            accounts_by_id = defaultdict(list)
            for user_accounts in accounts_by_type[service_type]:
                accounts_by_id[user_accounts["account_id"]].append(user_account)
            sql = "SELECT * FROM content_%saccount WHERE account_id IN (%s)" % (service_type, ",".join(accounts_by_id.keys()))
            try:
                data = yield self.mysql.runQuery(sql)
            except Exception, e:
                logger.error("Could not find service %s, %s" % (service_type, sql))
                continue
            for service_credentials in data:
                user_account = accounts_by_id[service_credentials["account_id"]]
                job = Job(
                    function_name=user_account['type'],
                    service_credentials=service_credentials,
                    user_account=user_account,
                    uuid=user_account['uuid'])
                self.mapJob(job) # Do this now so mapped values are cached.
                self._setJobCache(job)
                self.job_queue.append(job)

    def executeJobs(self):
        for i in range(0, self.simultaneous_reqs - self.rq.total_active_reqs):
            if not len(self.job_queue):
                return
            self.executeJob(self.job_queue.pop(0))

    @inlineCallbacks
    def executeJob(self, job):

        plugin = job.function_name.split('/')[0]
        dotted_function = '.'.join(job.function_name.split('/'))
        try:
            yield super(WorkerServer, self).executeJob(job)
            self.saveJobHistory(job, True)
            self.jobs_complete += 1
        except DeleteReservationException:
            self.jobs_complete += 1
            yield self.deleteReservation(job.uuid)
        except StaleContentException:
            self.jobs_complete += 1
        except QueueTimeoutException, e:
            self.job_failures += 1
            logger.error("Queue timeout for %s" % job.subservice)
            self.stats.increment('job.%s.queuetimeout' % dotted_function)
            self.stats.increment('pg.queuetimeout.hit', 0.05)
            self.saveJobHistory(job, False)
        except NegativeCacheException, e:
            self.jobs_complete += 1
            logger.error("Negative cache for %s" % job.subservice)
            if isinstance(e, NegativeReqCacheException):
                self.stats.increment('job.%s.negreqcache' % dotted_function)
            else:
                self.stats.increment('job.%s.negcache' % dotted_function)
            self.saveJobHistory(job, False)
        except Exception, e:
            self.job_failures += 1
            plugl = logging.getLogger(plugin)
            plugl.error("Error executing job:\n%s\n%s" % (job, format_exc()))
            self.stats.increment('job.exceptions', 0.1)
            self.saveJobHistory(job, False)
        yield self.clearPageCache(job)

    @inlineCallbacks
    def getFastCache(self, uuid):
        try:
            data = yield self.redis_client.get("fastcache:%s" % uuid)
        except:
            logger.debug("Could not get Fast Cache for %s" % uuid)
        returnValue(data)

    @inlineCallbacks
    def setFastCache(self, uuid, data):
        if not isinstance(data, str):
            raise Exception("FastCache must be a string.")
        if uuid is None:
            return
        try:
            yield self.redis_client.set("fastcache:%s" % uuid, data)
            logger.debug("Successfully set fast cache for %s" % uuid)
        except Exception, e:
            logger.error("Could not set fast cache: %s" % e)

    def logStatus(self):
        logger.critical('UUIDs: %d' % len(self.uuid_queue))
        logger.critical('Uncached UUIDs: %d' % len(self.uncached_uuid_queue))
        logger.critical('User accounts: %d' % len(self.user_account_queue))
        logger.critical('Queued Jobs: %d' % len(self.job_queue))
        logger.critical('Active Jobs: %d' % len(self.active_jobs))
        logger.critical('Completed Jobs: %d' % self.jobs_complete)
        logger.critical('Job failures: %d' % self.job_failures)
        logger.critical('Lost jobs: %d' % (self.uuids_dequeued - self.jobs_complete - self.job_failures - len(self.uuid_queue) - len(self.uncached_uuid_queue) - len(self.user_account_queue) - len(self.job_queue) - len(self.active_jobs)))
