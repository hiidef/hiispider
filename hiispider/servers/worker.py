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

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)
logger = logging.getLogger(__name__)


class WorkerServer(CassandraServer, JobQueueMixin, PageCacheQueueMixin, JobGetterMixin, JobHistoryMixin):

    public_ip = None
    local_ip = None
    network_information = {}
    simultaneous_reqs = 50
    jobs_complete = 0
    job_queue = []
    jobsloop = None
    dequeueloop = None
    uuid_queue = []
    uuid_dequeueing = False
    uuid_queue_size = 2000
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

    @inlineCallbacks
    def shutdown(self):
        self.jobsloop.stop()
        self.dequeueloop.stop()
        yield self.stopJobQueue()
        yield self.stopPageCacheQueue()
        yield super(WorkerServer, self).shutdown()
    
    @inlineCallbacks
    def dequeue_uuids(self):
        if self.uuid_dequeueing:
            return
        self.uuid_dequeueing = True
        while len(self.uuid_queue) < self.uuid_queue_size:
            msg = yield self.jobs_rabbit_queue.get()
            self.uuid_queue.append(UUID(bytes=msg.content.body).hex)
        self.uuid_dequeueing = False

    @inlineCallbacks
    def check_job_cache(self):
        if self.uncached_uuid_dequeueing:
            return
        self.uncached_uuid_dequeueing = True
        while self.uuid_queue:
            uuid = self.uuid_queue.pop(0)
            try:
                job = yield self._getCachedJob(uuid)
                self.job_queue.append(job)
            except:
                self.uncached_uuid_queue.append(uuid)
        self.uncached_uuid_dequeueing = False

    @inlineCallbacks
    def get_user_accounts(self):
        if self.user_account_dequeueing:
            return
        self.user_account_dequeueing = True
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
        self.user_account_dequeueing = False
 
    @inlineCallbacks
    def get_service_credentials(self):
        if self.service_credential_dequeueing or len(self.user_account_queue) < 1000:
            return
        self.service_credential_dequeueing = True
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
        self.service_credential_dequeueing = False

    def dequeue(self):
        self.dequeue_uuids()
        self.check_job_cache()
        self.get_user_accounts()
        self.get_service_credentials()

    def executeJobs(self):
        for i in range(0, self.simultaneous_reqs - self.rq.total_active_reqs):
            if not len(self.job_queue):
                return
            self.executeJob(self.job_queue.pop(0))

    @inlineCallbacks
    def executeJob(self, job):
        prev_complete = self.jobs_complete
        plugin = job.function_name.split('/')[0]
        dotted_function = '.'.join(job.function_name.split('/'))
        try:
            yield super(WorkerServer, self).executeJob(job)
            self.saveJobHistory(job, True)
            self.jobs_complete += 1
        except DeleteReservationException:
            yield self.deleteReservation(job.uuid)
        except StaleContentException:
            pass
        except QueueTimeoutException, e:
            self.stats.increment('job.%s.queuetimeout' % dotted_function)
            self.stats.increment('pg.queuetimeout.hit', 0.05)
            self.saveJobHistory(job, False)
        except NegativeCacheException, e:
            if isinstance(e, NegativeReqCacheException):
                self.stats.increment('job.%s.negreqcache' % dotted_function)
            else:
                self.stats.increment('job.%s.negcache' % dotted_function)
            self.saveJobHistory(job, False)
        except Exception, e:
            plugl = logging.getLogger(plugin)
            plugl.error("Error executing job:\n%s\n%s" % (job, format_exc()))
            self.stats.increment('job.exceptions', 0.1)
            self.saveJobHistory(job, False)
        if (prev_complete != self.jobs_complete) or len(self.active_jobs):
            self.logStatus()
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
        logger.debug('Completed Jobs: %d' % self.jobs_complete)
        logger.debug('Queued Jobs: %d' % len(self.job_queue))
        logger.debug('Active Jobs: %d' % len(self.active_jobs))

