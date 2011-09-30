from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy
from twisted.internet import task
import time
import logging


LOGGER = logging.getLogger(__name__)


def invert(d):
    """Invert a dictionary."""
    return dict([(v, k) for (k, v) in d.iteritems()])


class Job(object):

    mapped = False
    fast_cache = None
    uuid = None

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
        logger.debug("Creating job %s (%s) with kwargs %s and user %s" % (
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

    exposed_function_resources = {}
    active_jobs = {}
    reserved_arguments = [
        "reservation_function_name",
        "reservation_created",
        "reservation_next_request",
        "reservation_error"]
    functions = {}
    delta_functions = {}
    fast_cache = {}
    function_resource = None

    def __init__(self, server, config, address=None, **kwargs):
        super(JobExecuter, self).__init__(server, address=address)
        config = copy(config)
        config.update(kwargs)
        self.service_mapping = config["service_mapping"]
        self.service_args_mapping = config["service_args_mapping"]
        self.inverted_args_mapping = dict([(s[0], invert(s[1]))
            for s in self.service_args_mapping.items()])
        
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
            job = self.mapJob(job)
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
            self.stats.increment('job.%s.failure' % dotted_function)
            self.stats.timer.stop(timer)
            self.stats.timer.stop('job.time')
            raise
        finally:
            if job.uuid in self.active_jobs:
                del self.active_jobs[job.uuid]
        # If the data is None, there's nothing to store.
        # stats collection
        self.stats.increment('job.%s.success' % dotted_function)
        self.stats.timer.stop(timer)
        self.stats.timer.stop('job.time')
        returnValue(data)

    @inlineCallbacks
    def executeFunction(self, function_key, **kwargs):
        """Execute a function by key w/ kwargs and return the data."""
        logger.debug("Executing function %s with kwargs %r" % (function_key, kwargs))
        try:
            data = yield maybeDeferred(self.functions[function_key]['function'], **kwargs)
        except NegativeCacheException:
            raise
        except Exception, e:
            logger.error("Error with %s.\n%s" % (function_key, e))
            raise
        returnValue(data)

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
                logger.error(message)
                raise Exception(message)
        for key in optional_arguments:
            if key in self.reserved_arguments:
                message = "Optional argument name '%s' is reserved." % key
                logger.error(message)
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
        logger.info("Function %s is now callable." % function_name)
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
            logger.info("%s is now available via HTTP." % function_name)
        return function_name