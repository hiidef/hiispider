import urllib
from .cassandra import CassandraServer
from .base import LOGGER
from ..resources import WorkerResource
from .mixins import AMQPMixin, JobGetterMixin
from twisted.internet import reactor, task
from twisted.web import server
from twisted.internet.defer import inlineCallbacks, returnValue
import pprint
from traceback import format_exc
from ..exceptions import DeleteReservationException


PRETTYPRINTER = pprint.PrettyPrinter(indent=4)


class WorkerServer(CassandraServer, AMQPMixin, JobGetterMixin):

    public_ip = None
    local_ip = None
    network_information = {}
    simultaneous_jobs = 50
    jobs_complete = 0
    job_queue = []
    jobsloop = None
    dequeueloop = None
    queue_requests = 0

    def __init__(self, config, port=None):
        super(WorkerServer, self).__init__(config)
        self.setupAMQP(config)
        self.setupJobGetter(config)
        # HTTP interface
        resource = WorkerResource(self)
        if port is None:
            port = config["worker_server_port"]
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        self.scheduler_server = config["scheduler_server"]
        self.scheduler_server_port = config["scheduler_server_port"]

    def start(self):
        start_deferred = super(WorkerServer, self).start()
        start_deferred.addCallback(self._workerStart)
        return start_deferred

    @inlineCallbacks
    def _workerStart(self, started=None):
        LOGGER.debug("Starting worker components.")
        yield self.startJobQueue()
        yield self.startPageCacheQueue()
        self.jobsloop = task.LoopingCall(self.executeJobs)
        self.jobsloop.start(0.2)
        self.dequeueloop = task.LoopingCall(self.dequeue)
        self.dequeueloop.start(1)

    @inlineCallbacks
    def shutdown(self):
        self.jobsloop.stop()
        self.dequeueloop.stop()
        yield self.stopJobQueue()
        yield self.stopPageCacheQueue()
        yield super(WorkerServer, self).shutdown()

    def dequeue(self):
        self.logStatus()
        while len(self.job_queue) + self.queue_requests <= self.amqp_prefetch_count:
            self.queue_requests += 1
            LOGGER.debug('Fetching from queue, %s queue requests.' % self.queue_requests)
            self.dequeue_item()

    @inlineCallbacks
    def dequeue_item(self):
        try:
            uuid = yield self.getJobUUID()
        except Exception, e:
            LOGGER.error('Dequeue Error: %s' % e)
            return
        self.queue_requests -= 1
        LOGGER.debug('Got job %s' % uuid)
        try:
            job = yield self.getJob(uuid)
        except Exception, e:
            LOGGER.error('Job Error: %s\n%s' % (e, format_exc()))
            return
        if job.function_name in self.functions:
            LOGGER.debug('Successfully pulled job off of AMQP queue')
            if self.functions[job.function_name]["check_fast_cache"]:
                job.fast_cache = yield self.getFastCache(job.uuid)
            self.job_queue.append(job)
        else:
            LOGGER.error("Could not find function %s." % job.function_name)
            return

    def executeJobs(self):
        while len(self.job_queue) > 0 and len(self.active_jobs) < self.simultaneous_jobs:
            job = self.job_queue.pop(0)
            self.executeJob(job)

    @inlineCallbacks
    def executeJob(self, job):
        try:
            yield super(WorkerServer, self).executeJob(job)
            self.jobs_complete += 1
        except DeleteReservationException:
            yield self.deleteReservation(job.uuid)
        self.logStatus()
        yield self.clearPageCache(job)

    @inlineCallbacks
    def getFastCache(self, uuid):
        try:
            data = yield self.redis_client.get("fastcache:%s" % uuid)
        except:
            LOGGER.debug("Could not get Fast Cache for %s" % uuid)
        returnValue(data)

    @inlineCallbacks
    def setFastCache(self, uuid, data):
        if not isinstance(data, str):
            raise Exception("FastCache must be a string.")
        if uuid is None:
            return
        try:
            yield self.redis_client.set("fastcache:%s" % uuid, data)
            LOGGER.debug("Successfully set fast cache for %s" % uuid)
        except Exception, e:
            LOGGER.error("Could not set fast cache: %s" % e)

    def logStatus(self):
        LOGGER.debug('Completed Jobs: %d' % self.jobs_complete)
        LOGGER.debug('Queued Jobs: %d' % len(self.job_queue))
        LOGGER.debug('Active Jobs: %d' % len(self.active_jobs))
