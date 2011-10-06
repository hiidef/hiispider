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
import inspect

LOGGER = logging.getLogger(__name__)


def invert(d):
    """Invert a dictionary."""
    return dict([(v, k) for (k, v) in d.iteritems()])


class Job(object):

    mapped = False
    fast_cache = None
    uuid = None
    connected = False
    _dotted_function = None

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
        self._dotted_function = '.'.join(self.function_name.split('/'))

    def get_dotted_function(self):
        if not self._dotted_function:    
            self._dotted_function = '.'.join(self.function_name.split('/'))
        return self._dotted_function
    
    dotted_function = property(get_dotted_function)

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
    exposed_function_resources = {}
    function_resource = None
    functions = {}
    delta_functions = {}
    reserved_arguments = [
        "reservation_function_name",
        "reservation_created",
        "reservation_next_request",
        "reservation_error"]

    def __init__(self, server, config, address=None, **kwargs):
        super(JobExecuter, self).__init__(server, address=address)
        config = copy(config)
        config.update(kwargs)
        self.initialized = True
        self.service_mapping = config["service_mapping"]
        self.service_args_mapping = config["service_args_mapping"]
        self.inverted_args_mapping = dict([(s[0], invert(s[1]))
            for s in self.service_args_mapping.items()])

    def initialize(self):
        pass

    def start(self):
        pass

    def executeJob(self, job):
        timer = 'job.%s.duration' % (job.dotted_function)
        self.server.stats.timer.start(timer, 0.5)
        self.server.timer.start('job.time', 0.1)
        if not job.mapped:
            raise Exception("Unmapped job.")
        f = self.functions[job.function_name]
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
        self.server.stats.increment('job.%s.success' % dotted_function)
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
            if job.uuid:
                self.deleteReservation(job.uuid)
        except StaleContentException:
            self.jobs_complete += 1
        except QueueTimeoutException, e:
            self.job_failures += 1
            self.stats.increment('job.%s.queuetimeout' % job.dotted_function)
            self.stats.increment('pg.queuetimeout.hit', 0.05)
            self.saveJobHistory(job, False)
        except NegativeCacheException, e:
            self.jobs_complete += 1
            if isinstance(e, NegativeReqCacheException):
                self.stats.increment('job.%s.negreqcache' % job.dotted_function)
            else:
                self.stats.increment('job.%s.negcache' % job.dotted_function)
            self.saveJobHistory(job, False)
        except Exception, e:
            self.job_failures += 1
            self.server.stats.increment('job.%s.failure' % dotted_function)
            self.server.stats.timer.stop(timer)
            self.server.stats.timer.stop('job.time')
            plugin = job.function_name.split('/')[0]
            plugl = logging.getLogger(plugin)
            plugl.error("Error executing job:\n%s\n%s" % (job, format_exc()))
            self.stats.increment('job.exceptions', 0.1)
            self.saveJobHistory(job, False)

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
            f = self.functions[job.function_name]
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

    def delta(self, func, handler):
        self.delta_functions[id(func)] = handler

    def expose(self, *args, **kwargs):
        return self.makeCallable(expose=True, *args, **kwargs)

    def makeCallable(self, func, interval=0, name=None, expose=False, category=None):
        argspec = self._getArguments(func)
        required_arguments, optional_arguments = argspec[0], argspec[3]
        variadic = all(argspec[1:3])
        if variadic:
            required_arguments, optional_arguments = [], []
        # Reservation fast cache is stored on with the reservation
        if "fast_cache" in required_arguments:
            del required_arguments[required_arguments.index("fast_cache")]
            check_fast_cache = True
        elif "fast_cache" in optional_arguments:
            del optional_arguments[optional_arguments.index("fast_cache")]
            check_fast_cache = True
        else:
            check_fast_cache = False
        # Indicates whether to send the reservation's UUID to the function
        if "job_uuid" in required_arguments:
            del required_arguments[required_arguments.index("job_uuid")]
            get_job_uuid = True
        elif "job_uuid" in optional_arguments:
            del optional_arguments[optional_arguments.index("job_uuid")]
            get_job_uuid = True
        else:
            get_job_uuid = False
        # Get function name, usually class/method
        if name is not None:
            function_name = name
        elif hasattr(func, "im_class"):
            function_name = "%s/%s" % (func.im_class.__name__, func.__name__)
        else:
            function_name = func.__name__
        function_name = function_name.lower()
        # Make sure the function isn't using any reserved arguments.
        for key in required_arguments:
            if key in self.reserved_arguments:
                message = "Required argument name '%s' is reserved." % key
                LOGGER.error(message)
                raise Exception(message)
        for key in optional_arguments:
            if key in self.reserved_arguments:
                message = "Optional argument name '%s' is reserved." % key
                LOGGER.error(message)
                raise Exception(message)
        # Make sure we don't already have a function with the same name.
        if function_name in self.functions:
            raise Exception("Function %s is already callable." % function_name)
        # Add it to our list of callable functions.
        self.functions[function_name] = {
            "function":func,
            "id":id(func),
            "interval":interval,
            "required_arguments":required_arguments,
            "optional_arguments":optional_arguments,
            "check_fast_cache":check_fast_cache,
            "variadic":variadic,
            "get_job_uuid":get_job_uuid,
            "delta":self.delta_functions.get(id(func), None),
            "category":category,
        }
        LOGGER.info("Function %s is now callable." % function_name)
        if expose and self.function_resource is not None:
            self.exposed_functions.append(function_name)
            er = ExposedResource(self, function_name)
            function_name_parts = function_name.split("/")
            if len(function_name_parts) > 1:
                if function_name_parts[0] in self.exposed_function_resources:
                    r = self.exposed_function_resources[function_name_parts[0]]
                else:
                    r = Resource()
                    self.exposed_function_resources[function_name_parts[0]] = r
                self.function_resource.putChild(function_name_parts[0], r)
                r.putChild(function_name_parts[1], er)
            else:
                self.function_resource.putChild(function_name_parts[0], er)
            LOGGER.info("%s is now available via HTTP." % function_name)
        return function_name
    
    def _getArguments(self, func):
        """Get required or optional arguments for a plugin method.  This
        function returns a quadruple similar to inspect.getargspec (upon
        which it is based).  The argument ``self`` is always ignored.  If
        varargs or keywords (*args or **kwargs) are available, the caller
        should call call with all available arguments as kwargs.  If there
        are positional arguments required by the original function not
        present in the kwargs from the job, be sure to add that to the keyword
        arguments map in your spider config.  Unlike getargspec, the first element
        contains only required (positional) arguments, and the third element
        contains only keyword arguments in the argspec, not their defaults."""
        # this returns (args, varargs, keywords, defaults)
        argspec = list(inspect.getargspec(func))
        if argspec[0] and argspec[0][0] == 'self':
            argspec[0] = argspec[0][1:]
        args, defaults = argspec[0], argspec[3]
        defaults = [] if defaults is None else defaults
        argspec[0] = args[0:len(args) - len(defaults)]
        argspec[3] = args[len(args) - len(defaults):]
        return argspec

    def getPage(self, *args, **kwargs):
        return self.server.pagegetter.getPage(*args, **kwargs)

    def setHostMaxRequestsPerSecond(self, *args, **kwargs):
        return self.server.pagegetter.setHostMaxRequestsPerSecond(*args, **kwargs)

    def setHostMaxSimultaneousRequests(self, *args, **kwargs):
        return self.server.pagegetter.setHostMaxSimultaneousRequests(*args, **kwargs)
    
