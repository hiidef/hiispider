#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Connects to various components to generate an internal queue of job objects."""

import urllib
import logging
import cPickle
from zlib import decompress, compress
from uuid import UUID
from collections import defaultdict
from traceback import format_exc
from copy import copy
from twisted.internet import task
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue
from txamqp.queue import Empty
from hiispider.exceptions import JobGetterShutdownException
from hiispider.components import Redis, MySQL, JobQueue, Logger, Component,\
        shared, Queue
from hiispider.job import Job
from .base import MetaComponent

LOGGER = logging.getLogger(__name__)


class JobGetter(MetaComponent):

    dequeueloop = None
    uuid_queue = []
    uncached_uuid_queue = []
    user_account_queue = []
    fast_cache_queue = []
    job_queue = []
    uuid_dequeueing = False
    min_size = 1000
    job_requests = []
    requires = [Redis, MySQL, JobQueue, Logger]

    def __init__(self, server, config, server_mode, **kwargs):
        super(JobGetter, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        # add the ability to configure min_size
        self.min_size = config.get('jobgetter_min_size', JobGetter.min_size)
        self.scheduler_server = config["scheduler_server"]
        self.scheduler_server_port = config["scheduler_server_port"]

    def __len__(self):
        return sum([
            len(self.uuid_queue),
            len(self.uncached_uuid_queue),
            len(self.fast_cache_queue),
            len(self.job_queue)])

    def start(self):
        self.dequeueloop = task.LoopingCall(self.dequeue)
        self.dequeueloop.start(2)
        super(JobGetter, self).start()

    @inlineCallbacks
    def shutdown(self):
        for req in self.job_requests:
            req.errback(JobGetterShutdownException("JobGetter shut down."))
        self.uuid_queue = []
        self.uncached_uuid_queue = []
        self.user_account_queue = []
        yield super(JobGetter, self).shutdown()

    @shared
    @inlineCallbacks
    def deleteJobCache(self, uuid):
        yield self.server.redis.delete(uuid)

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
    def delete(self, uuid):
        """Delete a reservation by uuid."""
        yield self.server.mysql.runQuery("DELETE FROM spider_service "
            "WHERE uuid=%s", uuid)
        url = 'http://%s:%s/jobscheduler/remove_uuid?%s' % (
            self.scheduler_server,
            self.scheduler_server_port,
            urllib.urlencode({'uuid': uuid}))
        try:
            yield self.server.rq.getPage(url=url)
        except:
            LOGGER.error(format_exc())
        yield self.server.cassandra.remove(
            uuid,
            self.server.cassandra.cf_content)
        returnValue({'success': True})

    @shared
    def getJobs(self):
        if self.job_queue:
            jobs, self.job_queue = self.job_queue[0:100], self.job_queue[100:]
            return jobs
        else:
            d = Deferred()
            self.job_requests.append(d)
            return d

    def dequeue(self):
        LOGGER.debug("%s queued jobs." % len(self.job_queue))
        while self.job_requests:
            if self.job_queue:
                jobs, self.job_queue = self.job_queue[0:100], self.job_queue[100:]
                d = self.job_requests.pop(0)
                d.callback(jobs)
            else:
                break
        if self.uncached_uuid_queue:
            self.lookupjobs()
        if self.fast_cache_queue:
            self.getFastCache()
        if self.uuid_dequeueing:
            return
        if len(self) > self.min_size:
            return
        self._dequeuejobs()

    @inlineCallbacks
    def _dequeuejobs(self):
        self.uuid_dequeueing = True
        while len(self) < self.min_size * 4:
            content = None
            try:
                content = yield self.server.jobqueue.get(timeout=5)
            except Empty:
                pass
            except Exception, e:
                LOGGER.error(format_exc())
                break
            if content:
                self.uuid_queue.append(UUID(bytes=content).hex)
                if len(self.uuid_queue) > self.min_size / 2:
                    uuids, self.uuid_queue = self.uuid_queue, []
                    try:
                        data = yield self.server.redis.mget(*uuids)
                    except Exception:
                        LOGGER.error(format_exc())
                        break
                    self.checkCachedJobs(zip(uuids, data))
        self.uuid_dequeueing = False

    def checkCachedJobs(self, results):
        for row in results:
            if row[1]:
                try:
                    job = cPickle.loads(decompress(row[1]))
                except Exception:
                    self.uncached_uuid_queue.append(row[0])
                    continue
                if job.function_name not in self.server.functions:
                    LOGGER.error("Unknown service type %s" % job.function_name)
                    continue
                if not isinstance(job, Job):
                    self.uncached_uuid_queue.append(row[0])
                    continue
                self.fast_cache_queue.append(job)
            else:
                self.uncached_uuid_queue.append(row[0])

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
            if service_type not in self.server.functions:
                LOGGER.error("Unknown service type %s" % service_type)
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


