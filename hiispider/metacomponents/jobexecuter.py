from ..components.base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue
from copy import copy
from twisted.internet import task
import time
import logging
from hiispider.exceptions import NegativeCacheException

import os
import time
import pprint
from decimal import Decimal
from uuid import uuid4
from MySQLdb import OperationalError
from twisted.web.resource import Resource


LOGGER = logging.getLogger(__name__)


def invert(d):
    """Invert a dictionary."""
    return dict([(v, k) for (k, v) in d.iteritems()])


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

    def __init__(self, server, config, address=None, **kwargs):
        super(JobExecuter, self).__init__(server, address=address)
        config = copy(config)
        config.update(kwargs)
        self.initialized = True
        
    def initialize(self):
        pass

    def start(self):
        pass

    @inlineCallbacks
    def executeJob(self, job):
        dotted_function = '.'.join(job.function_name.split('/'))
        timer = 'job.%s.duration' % (dotted_function)
        self.server.stats.timer.start(timer, 0.5)
        self.server.timer.start('job.time', 0.1)
        if not job.mapped:
            raise Exception("Unmapped job.")
        f = self.functions[job.function_name]
        if job.uuid is not None:
            self.active_jobs[job.uuid] = True
        if f["get_job_uuid"]:
            job.kwargs["job_uuid"] = job.uuid
        if f["check_fast_cache"]:
            job.kwargs["fast_cache"] = job.fast_cache
        try:
            data = yield self.executeFunction(job.function_name, **job.kwargs)
        except NegativeCacheException:
            raise
        except Exception, e:
            self.server.stats.increment('job.%s.failure' % dotted_function)
            self.server.stats.timer.stop(timer)
            self.server.stats.timer.stop('job.time')
            raise
        finally:
            if job.uuid in self.active_jobs:
                del self.active_jobs[job.uuid]
        # If the data is None, there's nothing to store.
        # stats collection
        self.server.stats.increment('job.%s.success' % dotted_function)
        self.server.stats.timer.stop(timer)
        self.server.stats.timer.stop('job.time')
        returnValue(data)

    @inlineCallbacks
    def executeFunction(self, function_key, **kwargs):
        """Execute a function by key w/ kwargs and return the data."""
        LOGGER.debug("Executing function %s with kwargs %r" % (function_key, kwargs))
        try:
            data = yield maybeDeferred(self.functions[function_key]['function'], **kwargs)
        except NegativeCacheException:
            raise
        except Exception, e:
            LOGGER.error("Error with %s.\n%s" % (function_key, e))
            raise
        returnValue(data)

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
        logger.debug("Got server data:\n%s" % pprint.pformat(data))
        return data

