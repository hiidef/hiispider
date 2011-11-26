#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Job execution base class."""


from copy import copy
import time
import logging
import zlib
import os
import pprint
from decimal import Decimal
import ujson as json
from traceback import format_tb, format_exc

from twisted.internet.defer import inlineCallbacks, maybeDeferred
from twisted.internet.error import ConnectionRefusedError, TimeoutError
from twisted.web.error import Error as TwistedWebError

from hiispider.metacomponents.jobgetter import JobGetter
from hiispider.metacomponents.pagegetter import PageGetter
from hiispider.exceptions import *
from .base import MetaComponent
from hiispider.components import Stats, MySQL, JobHistoryRedis
from hiispider.components import Cassandra, Logger, PageCacheQueue
from hiispider.components.base import ComponentException


LOGGER = logging.getLogger(__name__)


def invert(d):
    """Invert a dictionary."""
    return dict([(v, k) for (k, v) in d.iteritems()])


class JobExecuter(MetaComponent):

    active_jobs = {}
    fast_cache = {}
    start_time = time.time()
    jobs_complete = 0
    job_failures = 0
    allow_clients = False
    requires = [Stats, MySQL, JobHistoryRedis, JobGetter, PageGetter, PageCacheQueue, Cassandra, Logger]

    def __init__(self, server, config, server_mode, **kwargs):
        super(JobExecuter, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        self.initialized = True
        self.service_mapping = config["service_mapping"]
        self.service_args_mapping = config["service_args_mapping"]
        self.inverted_args_mapping = dict([(s[0], invert(s[1]))
            for s in self.service_args_mapping.items()])
        self.delta_debug = config.get('delta_debug', False)
        self.mysql = self.server.mysql  # For legacy plugins.

    def executeJob(self, job):
        timer = 'job.%s.duration' % job.dotted_name
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
        try:
            del self.active_jobs[job.uuid]
        except:
            pass
        self.server.jobhistoryredis.save(job, True)
        self.jobs_complete += 1
        self.server.pagecachequeue.clear(job)
        self.server.stats.increment('job.%s.success' % job.dotted_name)
        self.server.stats.timer.stop(timer)
        self.server.stats.timer.stop('job.time')
        return data

    def _executeJobErrback(self, error, job, timer):
        try:
            del self.active_jobs[job.uuid]
        except:
            pass
        try:
            error.raiseException()
        except DeleteReservationException:
            self.jobs_complete += 1
            self.server.jobgetter.delete(job.uuid)
        except JobGetterShutdownException, e:
            LOGGER.info(e)
        except StaleContentException:
            self.jobs_complete += 1
        except QueueTimeoutException, e:
            self.job_failures += 1
            self.server.stats.increment('job.%s.queuetimeout' % job.dotted_name)
            self.server.stats.increment('pg.queuetimeout.hit', 0.05)
            self.server.jobhistoryredis.save(job, False)
        except NegativeCacheException, e:
            self.jobs_complete += 1
            if isinstance(e, NegativeReqCacheException):
                self.server.stats.increment('job.%s.negreqcache' % job.dotted_name)
            else:
                self.server.stats.increment('job.%s.negcache' % job.dotted_name)
            self.server.jobhistoryredis.save(job, False)
        except (TimeoutError, ConnectionRefusedError, TwistedWebError), e:

            self.job_failures += 1
            self.server.stats.increment('job.%s.failure' % job.dotted_name)
            self.server.stats.increment('job.exceptions', 0.1)
            self.server.jobhistoryredis.save(job, False)
        except Exception, e:
            self.server.jobgetter.deleteJobCache(job.uuid)
            self.job_failures += 1
            self.server.stats.increment('job.%s.failure' % job.dotted_name)
            plugin = job.function_name.split('/')[0]
            plugl = logging.getLogger(plugin)
            tb = '\n'.join(format_tb(error.getTracebackObject()))
            plugl.error("%s: Error executing job:%s - %s\n%s\n%s" % (
                e.__class__,
                job.function_name,
                job.uuid,
                tb,
                format_exc()))
            self.server.stats.increment('job.exceptions', 0.1)
            self.server.jobhistoryredis.save(job, False)
        self.server.stats.timer.stop(timer)
        self.server.stats.timer.stop('job.time')

    @inlineCallbacks
    def generate_deltas(self, new_data, job, save=True):
        delta_func = self.server.functions[job.function_name]["delta"]
        if not delta_func:
            return
        old_data = yield self.server.cassandra.getData(job, consistency=2)
        if not old_data:
            return

        # make sure old_data and new_data are similar (strings are all unicode, ect)

        # Taking this out as encoding and decoding should be done by the same
        # module (now ujson) and encoding / decoding is CPU intensive.
        # Note that the autodelta now compensates for unicode / str differences
        # - JDW

        # # new_data = json.decode(json.encode(new_data))

        # get deltas by comparing new and old data sets
        deltas = delta_func(new_data, old_data)
        for delta in deltas:
            category = self.server.functions[job.function_name].get('category', 'unknown')
            if not category:
                category = 'unknown'
            user_id = str(job.user_account['user_id'])
            user_column = b'%s||%s||%s||%s' % (delta.id, category, job.subservice, job.user_account['account_id'])
            mapping = {
                'data': zlib.compress(json.dumps(delta.data)),
                'user_id': user_id,
                'category': category,
                'service': job.subservice.split('/')[0],
                'subservice': job.subservice,
                'uuid': job.uuid,
                "path": delta.path}
            if self.delta_debug:
                ts = str(time.time())
                mapping.update({
                    'old_data': zlib.compress(json.dumps(old_data)),
                    'new_data': zlib.compress(json.dumps(new_data)),
                    'generated': ts,
                    'updated': ts})
            if save:
                yield self.server.cassandra.batch_insert(
                    key=str(delta.id),
                    column_family=self.server.cassandra.cf_delta,
                    mapping=mapping)
                yield self.server.cassandra.insert(
                    key=user_id,
                    column_family=self.server.cassandra.cf_delta_user,
                    column=user_column,
                    value='')

    def getServerData(self):
        running_time = time.time() - self.start_time
        active_requests_by_host = self.rq.getActiveRequestsByHost()
        pending_requests_by_host = self.rq.getPendingRequestsByHost()
        data = {
            "load_avg": [str(Decimal(str(x), 2)) for x in os.getloadavg()],
            "running_time": running_time,
            "active_requests_by_host": active_requests_by_host,
            "pending_requests_by_host": pending_requests_by_host,
            "active_requests": self.rq.getActive(),
            "pending_requests": self.rq.getPending()
        }
        LOGGER.debug("Got server data:\n%s" % pprint.pformat(data))
        return data

    def mapJob(self, job):
        if job.function_name in self.service_mapping:
            job.function_name = self.service_mapping[job.function_name]
            job.dotted_name = job.function_name.replace("/", ".")
        service_name = job.function_name.split('/')[0]
        if service_name in self.inverted_args_mapping:
            kwargs = {}
            mapping = self.inverted_args_mapping[service_name]
            f = self.server.functions[job.function_name]
            # add in support for completely variadic methods;  these are
            # methods that accept *args, **kwargs in some fashion (usually
            # because of a decorator like inlineCallbacks);  note that these
            # will be called with the full amt of kwargs pulled in by the
            # jobGetter and should therefore take **kwargs somewhere underneath
            # and have all of its real positional args mapped in the
            # inverted_args_mapping
            if f['variadic']:
                kwargs = dict(job.kwargs)
                for key, value in mapping.iteritems():
                    if value in kwargs:
                        kwargs[key] = kwargs.pop(value)
            else:
                for key in f['required_arguments']:
                    if key in mapping and mapping[key] in job.kwargs:
                        kwargs[key] = job.kwargs[mapping[key]]
                    elif key in job.kwargs:
                        kwargs[key] = job.kwargs[key]
                    # mimic the behavior of the old job mapper,
                    # mapping args (like 'type') to the spider_service object
                    # itself in addition to the job kwargs
                    elif key in job.user_account:
                        kwargs[key] = job.user_account[key]
                    else:
                        LOGGER.error("Could not find required argument %s for "
                            " function %s in %s. Available: %s, %s" % (
                            key,
                            job.function_name,
                            job,
                            job.kwargs,
                            job.user_account))
                        # FIXME: we shouldn't except here because a
                        # log message and quiet failure is enough;
                        # we need some quiet error channel
                        raise Exception("Could not find argument: %s" % key)
                for key in f['optional_arguments']:
                    if key in mapping and mapping[key] in job.kwargs:
                        kwargs[key] = job.kwargs[mapping[key]]
                    elif key in job.kwargs:
                        kwargs[key] = job.kwargs[key]
            job.kwargs = kwargs
        job.mapped = True
        return job
