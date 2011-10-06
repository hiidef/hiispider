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


LOGGER = logging.getLogger(__name__)


class JobGetter(Component):

    dequeueloop = None
    uuid_queue = []
    uncached_uuid_queue = []
    user_account_queue = []
    job_queue = []
    uuid_dequeueing = False
    uuid_queue_size = 500
    job_queue_size = 500

    def __init__(self, server, config, address=None, **kwargs):
        super(JobGetter, self).__init__(server, address=address)
        config = copy(config)
        config.update(kwargs)

    def start(self):
        if self.server_mode:
            self.dequeueloop = task.LoopingCall(self.dequeue)
            self.dequeueloop.start(5)
            self.initialized = True
        
    @inlineCallbacks
    def shutdown(self):
        if self.dequeueloop:
            self.dequeueloop.stop()
        self.uuid_queue = []
        self.uncached_uuid_queue = []
        self.user_account_queue = []
        yield super(JobGetter, self).shutdown()
   
    def dequeue(self):
        LOGGER.debug("%s queued jobs." % len(self.job_queue))
        if self.uncached_uuid_queue:
            self.lookupjobs()
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
        if len(self.uuid_queue) > 200:
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
                LOGGER.debug('Found uuid in Redis: %s' % row[0])
                self.job_queue.append(job)
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
                self.server.mapJob(job) # Do this now so mapped values are cached.
                self._setJobCache(job)
                self.job_queue.append(job)

    @inlineCallbacks
    def _setJobCache(self, job):
        """Set job cache in redis. Expires at now + 7 days."""
        job_data = compress(cPickle.dumps(job), 1)
        # TODO: Figure out why txredisapi thinks setex doesn't like sharding.
        try:
            yield self.server.redis.set(job.uuid, job_data)
            yield self.server.redis.expire(job.uuid, 60*60*24*7)
        except Exception, e:
            logger.error(str(e))
