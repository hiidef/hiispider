from ..components.base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue, maybeDeferred
from copy import copy
from twisted.internet import task
import time
import logging
from hiispider.exceptions import NegativeCacheException
import zlib
import os
import time
import pprint
from decimal import Decimal
from uuid import uuid4
from MySQLdb import OperationalError
from twisted.web.resource import Resource

import simplejson
from traceback import format_tb, format_exc
from hiispider.exceptions import *


LOGGER = logging.getLogger(__name__)
DOTTED_FUNCTION_NAMES = {}


def invert(d):
    """Invert a dictionary."""
    return dict([(v, k) for (k, v) in d.iteritems()])


def dotted(job):
    try:
        return DOTTED_FUNCTION_NAMES[job.function_name]
    except Exception, e:
        pass
    DOTTED_FUNCTION_NAMES[job.function_name] = job.function_name.replace("/", ".")
    return DOTTED_FUNCTION_NAMES[job.function_name]


class Job(object):

    mapped = False
    fast_cache = None
    uuid = None
    connected = False

    def __init__(self,
            function_name,
            service_credentials,
            user_account,
            uuid=None):
        """An encaspulated job object.  The ``function_name`` is its path on
        the http interface of the spider, the ``service_credentials`` (called
        ``kwargs`` on the job) are colums from the ``content_(service)account``
        table, and the ``user_account`` is a row of the ``spider_service``
        table along with the user's flavors username and chosen DNS host."""
        LOGGER.debug("Creating job %s (%s) with kwargs %s and user %s" % (
            uuid, function_name, service_credentials, user_account))
        self.function_name = function_name
        self.kwargs = service_credentials
        self.subservice = function_name
        self.uuid = uuid
        self.user_account = user_account

    def __repr__(self):
        return '<job: %s(%s)>' % (self.function_name, self.uuid)

    def __str__(self):
        return 'Job %s: \n%s' % (self.uuid, pprint.pformat(self.__dict__))


class JobExecuter(Component):

    active_jobs = {}
    fast_cache = {}
    start_time = time.time()
    jobs_complete = 0
    job_failures = 0

    def __init__(self, server, config, address=None, **kwargs):
        super(JobExecuter, self).__init__(server, address=address)
        config = copy(config)
        config.update(kwargs)
        self.initialized = True
        self.service_mapping = config["service_mapping"]
        self.service_args_mapping = config["service_args_mapping"]
        self.inverted_args_mapping = dict([(s[0], invert(s[1]))
            for s in self.service_args_mapping.items()])
        self.cassandra_cf_content = config["cassandra_cf_content"]            
        self.delta_debug = config.get('delta_debug', False)
        self.mysql = self.server.mysql # For legacy plugins.

    def initialize(self):
        pass

    def start(self):
        pass

    def executeJob(self, job):
        timer = 'job.%s.duration' % dotted(job)
        self.server.stats.timer.start(timer, 0.5)
        self.server.stats.timer.start('job.time', 0.1)
        if not job.mapped:
            raise Exception("Unmapped job.")
        f = self.server.functions[job.function_name]
        if job.uuid:
            self.active_jobs[job.uuid] = True
        if f["get_job_uuid"]:
            job.kwargs["job_uuid"] = job.uuid
        if f["check_fast_cache"]:
            job.kwargs["fast_cache"] = job.fast_cache
        d = maybeDeferred(f['function'], **job.kwargs)
        d.addCallback(self._executeJobCallback, job, timer)
        d.addErrback(self._executeJobErrback, job, timer)
        return d

    def _executeJobCallback(self, data, job, timer):
        self.server.jobhistoryredis.save(job, True)
        self.jobs_complete += 1
        self.server.pagecachequeue.clear(job)
        self.server.stats.increment('job.%s.success' % dotted(job))
        self.server.stats.timer.stop(timer)
        self.server.stats.timer.stop('job.time')
        return data      

    def _executeJobErrback(self, error, job, timer):
        if job.uuid in self.active_jobs:
            del self.active_jobs[job.uuid]
        try:
            error.raiseException()
        except DeleteReservationException:
            self.jobs_complete += 1
            self.server.jobgetter.delete(job)
        except JobGetterShutdownException, e:
            LOGGER.info(e)
        except StaleContentException:
            self.jobs_complete += 1
        except QueueTimeoutException, e:
            self.job_failures += 1
            self.server.stats.increment('job.%s.queuetimeout' % dotted(job))
            self.server.stats.increment('pg.queuetimeout.hit', 0.05)
            self.server.jobhistoryredis.save(job, False)
        except NegativeCacheException, e:
            self.jobs_complete += 1
            if isinstance(e, NegativeReqCacheException):
                self.server.stats.increment('job.%s.negreqcache' % dotted(job))
            else:
                self.server.stats.increment('job.%s.negcache' % dotted(job))
            self.server.jobhistoryredis.save(job, False)
        except Exception, e:
            self.job_failures += 1
            self.server.stats.increment('job.%s.failure' % dotted(job))
            self.server.stats.timer.stop(timer)
            self.server.stats.timer.stop('job.time')
            plugin = job.function_name.split('/')[0]
            plugl = logging.getLogger(plugin)
            tb = '\n'.join(format_tb(error.getTracebackObject()))
            plugl.error("Error executing job:\n%s\n%s\n%s" % (job, tb, format_exc()))
            self.server.stats.increment('job.exceptions', 0.1)
            self.server.jobhistoryredis.save(job, False)

    @inlineCallbacks
    def getData(self, job):
        try:
            data = yield self.server.cassandra.get(
                key=str(job.user_account["user_id"]),
                column_family=self.cassandra_cf_content,
                column=job.uuid)
            returnValue(simplejson.loads(zlib.decompress(data.column.value)))
        except NotFoundException:
            return
    
    def setData(self, data, job):
        return self.server.cassandra.insert(
            str(job.user_account["user_id"]),
            self.cassandra_cf_content,
            zlib.compress(simplejson.dumps(data)),
            column=job.uuid)
    
    @inlineCallbacks
    def generate_deltas(self, new_data, job):
        delta_func = self.server.functions[job.function_name]["delta"]
        if not delta_func:
            return
        old_data = yield self.getData(job)
        if not old_data:
            return
        deltas = delta_func(new_data, old_data)
        for delta in deltas:
            category = self.functions[job.function_name].get('category', 'unknown')
            user_column = b'%s:%s:%s' % (delta.id, category, job.subservice)
            mapping = {
                'data': zlib.compress(simplejson.dumps(delta.data)),
                'user_id': str(user_id),
                'category': category,
                'service': job.subservice.split('/')[0],
                'subservice': job.subservice,
                'uuid': job.uuid,
                "path": delta.path}
            if self.delta_debug:
                ts = str(time.time())
                mapping.update({
                    'old_data': zlib.compress(simplejson.dumps(old_data)),
                    'new_data': zlib.compress(simplejson.dumps(new_data)),
                    'generated': ts,
                    'updated': ts})
            yield self.server.cassandra.batch_insert(
                key=str(delta.id),
                column_family=self.cassandra_cf_delta,
                mapping=mapping)
            yield self.server.cassandra.insert(
                key=str(user_id),
                column_family=self.cassandra_cf_delta_user,
                column=user_column,
                value='')

    def getServerData(self):
        running_time = time.time() - self.start_time
        active_requests_by_host = self.rq.getActiveRequestsByHost()
        pending_requests_by_host = self.rq.getPendingRequestsByHost()
        data = {
            "load_avg":[str(Decimal(str(x), 2)) for x in os.getloadavg()],
            "running_time":running_time,
            "active_requests_by_host":active_requests_by_host,
            "pending_requests_by_host":pending_requests_by_host,
            "active_requests":self.rq.getActive(),
            "pending_requests":self.rq.getPending()
        }
        LOGGER.debug("Got server data:\n%s" % pprint.pformat(data))
        return data
    
    def mapJob(self, job):
        if job.function_name in self.service_mapping:
            LOGGER.debug('Remapping resource %s to %s' % (
                job.function_name,
                self.service_mapping[job.function_name]))
            job.function_name = self.service_mapping[job.function_name]
        service_name = job.function_name.split('/')[0]
        if service_name in self.inverted_args_mapping:
            kwargs = {}
            mapping = self.inverted_args_mapping[service_name]
            f = self.server.functions[job.function_name]
            # add in support for completely variadic methods;  these are methods
            # that accept *args, **kwargs in some fashion (usually because of a
            # decorator like inlineCallbacks);  note that these will be called
            # with the full amt of kwargs pulled in by the jobGetter and should
            # therefore take **kwargs somewhere underneath and have all of its
            # real positional args mapped in the inverted_args_mapping
            if f['variadic']:
                kwargs = dict(job.kwargs)
                for key,value in mapping.iteritems():
                    if value in kwargs:
                        kwargs[key] = kwargs.pop(value)
            else:
                for key in f['required_arguments']:
                    if key in mapping and mapping[key] in job.kwargs:
                        kwargs[key] = job.kwargs[mapping[key]]
                    elif key in job.kwargs:
                        kwargs[key] = job.kwargs[key]
                    # mimic the behavior of the old job mapper, mapping args (like 'type')
                    # to the spider_service object itself in addition to the job kwargs
                    elif key in job.user_account:
                        kwargs[key] = job.user_account[key]
                    else:
                        LOGGER.error('Could not find required argument %s for function %s in %s. Available: %s, %s' % (
                            key, job.function_name, job, job.kwargs, job.user_account))
                        # FIXME: we shouldn't except here because a log message and quiet
                        # failure is enough;  we need some quiet error channel
                        raise Exception("Could not find argument: %s" % key)
                for key in f['optional_arguments']:
                    if key in mapping and mapping[key] in job.kwargs:
                        kwargs[key] = job.kwargs[mapping[key]]
                    elif key in job.kwargs:
                        kwargs[key] = job.kwargs[key]
            job.kwargs = kwargs
        job.mapped = True
        return job

    def expose(self, *args, **kwargs):
        return self.server.expose(*args, **kwargs)

    def make_callable(self, *args, **kwargs):
        return self.server.make_callable(*args, **kwargs)

    def delta(self, *args, **kwargs):
        return self.server.delta(*args, **kwargs)

    def getPage(self, *args, **kwargs):
        return self.server.pagegetter.getPage(*args, **kwargs)

    def setHostMaxRequestsPerSecond(self, *args, **kwargs):
        return self.server.pagegetter.setHostMaxRequestsPerSecond(*args, **kwargs)

    def setHostMaxSimultaneousRequests(self, *args, **kwargs):
        return self.server.pagegetter.setHostMaxSimultaneousRequests(*args, **kwargs)
    
    def setFastCache(self, *args, **kwargs):
        return self.server.jobgetter.setFastCache(*args, **kwargs)