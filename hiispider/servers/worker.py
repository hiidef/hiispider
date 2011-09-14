import logging
import time

from .cassandra import CassandraServer
from ..resources import WorkerResource
from hiispider.servers.mixins import JobQueueMixin, PageCacheQueueMixin, JobGetterMixin, JobHistoryMixin
from hiispider.requestqueuer import QueueTimeoutException
from twisted.internet import reactor, task
from twisted.web import server
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredSemaphore, DeferredList
import pprint
from traceback import format_exc
from hiispider.exceptions import *

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)
logger = logging.getLogger(__name__)


class WorkerServer(CassandraServer, JobQueueMixin, PageCacheQueueMixin, JobGetterMixin, JobHistoryMixin):

    public_ip = None
    local_ip = None
    network_information = {}
    jobs_complete = 0
    job_queue_size = 1000
    worker_running = False
    jobs_semaphore = DeferredSemaphore(50)

    def __init__(self, config, port=None):
        super(WorkerServer, self).__init__(config)
        self.t0 = time.time()
        self.setupJobQueue(config)
        self.setupPageCacheQueue(config)
        self.setupJobGetter(config)
        self.request_chunk_size = config.get("amqp_prefetch_count", 10)
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
        self.running = True
        logger.debug("Starting worker components.")
        yield self.startJobQueue()
        yield self.startPageCacheQueue()
        yield self.setupJobHistory(self.config)
        self.worker_running = True
        self.dequeue()

    @inlineCallbacks
    def shutdown(self):
        self.worker_running = False
        yield self.stopJobQueue()
        yield self.stopPageCacheQueue()
        yield super(WorkerServer, self).shutdown()

    @inlineCallbacks
    def dequeue_item(self):
        try:
            uuid = yield self.getJobUUID()
        except Exception, e:
            # if we've started shutting down, ignore this error
            if self.shutdown_trigger_id is None:
                return
            logger.error('Dequeue Error: %s' % e)
            return
        logger.debug('Got job %s' % uuid)
        try:
            job = yield self.getJob(uuid)
        except Exception, e:
            logger.error('Job Error: %s\n%s' % (e, format_exc()))
            return
        # getJob can return None if it encounters an error that is not
        # exceptional, like seeing custom_* jobs in the scheduler
        if job is None:
            return
        if job.function_name in self.functions:
            logger.debug('Successfully pulled job off of AMQP queue')
            if self.functions[job.function_name]["check_fast_cache"]:
                job.fast_cache = yield self.getFastCache(job.uuid)
            d = self.jobs_semaphore.acquire()
            d.addCallback(self.executeJob, job)
        else:
            logger.error("Could not find function %s." % job.function_name)
            return

    @inlineCallbacks
    def dequeue(self):
        while len(self.jobs_semaphore.waiting) < self.job_queue_size:
            if not self.worker_running:
                return
            deferreds = []
            for i in range(0, self.request_chunk_size):
                deferreds.append(self.dequeue_item())
            yield DeferredList(deferreds, consumeErrors=True)
        reactor.callLater(1, self.dequeue)

    @inlineCallbacks
    def executeJob(self, semaphore, job):
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
        self.jobs_semaphore.release()

    @inlineCallbacks
    def getFastCache(self, uuid):
        try:
            data = yield self.redis_client.get("fastcache:%s" % uuid)
            returnValue(data)
        except:
            logger.debug("Could not get Fast Cache for %s" % uuid)

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
        logger.debug('Queued Jobs: %d' % len(self.jobs_semaphore.waiting))
        logger.debug('Active Jobs: %d' % len(self.active_jobs))

