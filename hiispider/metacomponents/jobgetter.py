from ..components.base import Component, shared
from ..components import Queue
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy
from twisted.internet import task
from jobexecuter import Job
import cPickle
from zlib import decompress, compress
from uuid import UUID
from collections import defaultdict
import logging
from traceback import format_exc
import urllib
from ..exceptions import *

LOGGER = logging.getLogger(__name__)

class JobGetter(Component):

    dequeueloop = None
    uuid_queue = []
    uncached_uuid_queue = []
    user_account_queue = []
    fast_cache_queue = []
    job_queue = []
    uuid_dequeueing = False
    uuid_queue_size = 50
    job_queue_size = 50
    job_requests = []

    def __init__(self, server, config, address=None, **kwargs):
        super(JobGetter, self).__init__(server, address=address)
        config = copy(config)
        config.update(kwargs)
        self.scheduler_server = config["scheduler_server"]
        self.scheduler_server_port = config["scheduler_server_port"]    
    
    @inlineCallbacks
    def setJobCache(self, job):
        """Set job cache in redis. Expires at now + 7 days."""
        job_data = compress(cPickle.dumps(job), 1)
        # TODO: Figure out why txredisapi thinks setex doesn't like sharding.
        try:
            yield self.server.redis.set(job.uuid, job_data)
            yield self.server.redis.expire(job.uuid, 60*60*24*7)
        except Exception, e:
            LOGGER.error(format_exc())

    @shared
    @inlineCallbacks
    def setFastCache(self, uuid, data):
        if not isinstance(data, str):
            raise Exception("FastCache must be a string.")
        if uuid is None:
            return
        try:    
            data = yield self.server.redis.set("fastcache:%s" % uuid, data)
        except Exception, e:
            LOGGER.error(format_exc())
        
    @shared
    @inlineCallbacks
    def deleteReservation(self, job):
        """Delete a reservation by uuid."""
        uuid = job.uuid
        LOGGER.info('Deleting job: %s, %s' % (job.function_name, job.uuid))
        yield self.server.mysql.runQuery('DELETE FROM spider_service WHERE uuid=%s', uuid)
        url = 'http://%s:%s/function/schedulerserver/removefromjobsheap?%s' % (
            self.scheduler_server,
            self.scheduler_server_port,
            urllib.urlencode({'uuid': uuid}))
        try:
            yield self.server.pagegetter.getPage(url=url, cache=-1)
        except:
            LOGGER.error(format_exc())
        yield self.self.cassandra.remove(
            uuid,
            self.cassandra_cf_content)
        returnValue({'success': True})

    @shared
    def getJob(self):
        if self.job_queue:
            return self.job_queue.pop(0)
        else:
            d = Deferred()
            self.job_requests.append(d)
            return d

    def start(self):
        if self.server_mode:
            self.dequeueloop = task.LoopingCall(self.dequeue)
            self.dequeueloop.start(5)
            self.initialized = True
        
    @inlineCallbacks
    def shutdown(self):
        for req in self.job_requests:
            req.errback(JobGetterShutdownException("JobGetter shut down."))
        self.uuid_queue = []
        self.uncached_uuid_queue = []
        self.user_account_queue = []
        yield super(JobGetter, self).shutdown()
   
    def dequeue(self):
        LOGGER.debug("%s queued jobs." % len(self.job_queue))
        while self.job_requests:
            if self.job_queue:
                d = self.job_requests.pop(0)
                d.callback(self.job_queue.pop(0))
            else:
                break
        if self.uncached_uuid_queue:
            self.lookupjobs()
        if self.fast_cache_queue:
            self.getFastCache()
        if self.uuid_dequeueing:
            return
        if len(self.job_queue) > self.job_queue_size or len(self.uuid_queue) > self.uuid_queue_size:
            return
        self.uuid_dequeueing = True
        self._dequeuejobs()

    def _dequeuejobs(self):
        if len(self.uuid_queue) < self.uuid_queue_size * 4 and len(self.job_queue) < self.job_queue_size * 4:
            d = self.server.jobqueue.get(timeout=5)
            d.addCallback(self._dequeuejobsCallback)
            d.addErrback(self._dequeuejobsErrback)
        else:
            self.uuid_dequeueing = False

    def _dequeuejobsCallback(self, msg):
        self.uuid_queue.append(UUID(bytes=msg.content.body).hex)
        if len(self.uuid_queue) > self.uuid_queue_size / 2:
            uuids, self.uuid_queue = self.uuid_queue, []
            d = self.server.redis.mget(*uuids)
            d.addCallback(self._dequeuejobsCallback2, uuids)
            d.addErrback(self._dequeuejobsErrback)
        else:
            self._dequeuejobs()

    def _dequeuejobsErrback(self, error):
        LOGGER.error(str(error))
        self.uuid_dequeueing = False

    def _dequeuejobsCallback2(self, data, uuids):
        results = zip(uuids, data)
        for row in results:
            if row[1]:
                job = cPickle.loads(decompress(row[1]))
                self.fast_cache_queue.append(job)
            else:
                LOGGER.debug('Could not find uuids %s in Redis.' % row[0])
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
                data = yield self.server.mysql.runQuery(sql)
            except Exception, e:
                LOGGER.error("Could not find users in '%s': %s" % ("','".join(uuids), e))
                continue
            self.user_account_queue.extend(data)
        accounts_by_type = defaultdict(list)
        for user_account in self.user_account_queue:
            accounts_by_type[user_account["type"]].append(user_account)
        self.user_account_queue = []
        for service_type in accounts_by_type:
            accounts_by_id = {}
            for user_account in accounts_by_type[service_type]:
                accounts_by_id[str(user_account["account_id"])] = user_account
            sql = "SELECT * FROM content_%saccount WHERE account_id IN (%s)" % (service_type.split("/")[0], ",".join(accounts_by_id.keys()))
            try:
                data = yield self.server.mysql.runQuery(sql)
            except Exception, e:
                LOGGER.error("Could not find service %s, %s: %s" % (service_type, sql, e))
                continue
            for service_credentials in data:
                user_account = accounts_by_id[str(service_credentials["account_id"])]
                job = Job(
                    function_name=user_account['type'],
                    service_credentials=service_credentials,
                    user_account=user_account,
                    uuid=user_account['uuid'])
                self.server.worker.mapJob(job) # Do this now so mapped values are cached.
                self.setJobCache(job)
                self.fast_cache_queue.append(job)
        
    @inlineCallbacks
    def getFastCache(self):
        fast_cache_jobs = []
        while self.fast_cache_queue:
            job = self.fast_cache_queue.pop(0)
            if self.server.functions[job.function_name]["check_fast_cache"]:
                fast_cache_jobs.append(job)
            else:
                self.job_queue.append(job)
        while fast_cache_jobs:
            jobs, fast_cache_jobs = fast_cache_jobs[0:200], fast_cache_jobs[200:]
            data = yield self.server.redis.mget(*[x.uuid for x in jobs])
            for row in zip(jobs, data):
                if row[1]:
                    row[0].fast_cache = row[1]
                self.job_queue.append(row[0])
