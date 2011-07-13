import inspect
import logging
import os
import time
import pprint

from decimal import Decimal
from uuid import uuid4
from MySQLdb import OperationalError
from twisted.web.resource import Resource
from twisted.enterprise import adbapi
from twisted.internet import reactor
from twisted.internet.defer import Deferred, maybeDeferred
from twisted.internet.defer import inlineCallbacks, returnValue

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)
logger = logging.getLogger(__name__)

from ..requestqueuer import RequestQueuer
from ..pagegetterlite import PageGetter
from ..resources import ExposedResource

# mock logd stats collection so that stats logged by this library do not cause
# any problems if a configured stats object is not provided

class Noop(object):
    def __getattr__(self, attr):
        return self.noop

    def noop(self, *args, **kwargs):
        return

class StatsNoop(Noop):
    def __init__(self):
        super(StatsNoop, self).__init__(self)
        self.timer = Noop()

    def timed(self, *args, **kwargs):
        def decorator(f):
            return f
        return decorator


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

    def __str__(self):
        return 'Job %s: \n%s' % (self.uuid, pprint.pformat(self.__dict__))

class BaseServer(object):

    exposed_functions = []
    exposed_function_resources = {}
    logging_handler = None
    shutdown_trigger_id = None
    uuid = uuid4().hex
    start_time = time.time()
    active_jobs = {}
    reserved_arguments = [
        "reservation_function_name",
        "reservation_created",
        "reservation_next_request",
        "reservation_error"]
    functions = {}
    delta_functions = {}
    categories = {}
    fast_cache = {}
    function_resource = None

    def __init__(self, config, pg=None):
        # Resource Mappings
        self.service_mapping = config["service_mapping"]
        self.service_args_mapping = config["service_args_mapping"]
        self.inverted_args_mapping = dict([(s[0], invert(s[1]))
            for s in self.service_args_mapping.items()])
        # Request Queuer
        self.rq = RequestQueuer(
            max_simultaneous_requests=config["max_simultaneous_requests"],
            max_requests_per_host_per_second=config["max_requests_per_host_per_second"],
            max_simultaneous_requests_per_host=config["max_simultaneous_requests_per_host"])
        self.rq.setHostMaxRequestsPerSecond("127.0.0.1", 0)
        self.rq.setHostMaxSimultaneousRequests("127.0.0.1", 0)
        if pg is None:
            self.pg = PageGetter(rq=self.rq)
        else:
            self.pg = pg

        self.stats = config.get('stats', StatsNoop())

    def start(self):
        start_deferred = Deferred()
        reactor.callWhenRunning(self._baseStart, start_deferred)
        return start_deferred

    def _baseStart(self, start_deferred):
        logger.debug("Starting Base components.")
        self.shutdown_trigger_id = reactor.addSystemEventTrigger(
            'before',
            'shutdown',
            self.shutdown)
        start_deferred.callback(True)

    @inlineCallbacks
    def shutdown(self):
        while self.rq.getPending() > 0 or self.rq.getActive() > 0:
            logger.debug("%s requests active, %s requests pending." % (
                self.rq.getPending(),
                self.rq.getActive()
            ))
            shutdown_deferred = Deferred()
            # Call the Deferred after a second to continue the loop.
            reactor.callLater(1, shutdown_deferred.callback)
            yield shutdown_deferred
        self.shutdown_trigger_id = None
        logger.critical("Server shut down.")
        logger.removeHandler(self.logging_handler)
        returnValue(True)

    def delta(self, func, handler):
        self.delta_functions[id(func)] = handler

    def expose(self, *args, **kwargs):
        return self.makeCallable(expose=True, *args, **kwargs)

    @inlineCallbacks
    def executeJob(self, job):
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
        except Exception, e:
            if job.uuid in self.active_jobs:
                del self.active_jobs[job.uuid]
            logger.debug("Received exception %s" % e)
            raise
        # If the data is None, there's nothing to store.
        if job.uuid in self.active_jobs:
            del self.active_jobs[job.uuid]
        returnValue(data)

    @inlineCallbacks
    def executeFunction(self, function_key, **kwargs):
        """Execute a function by key w/ kwargs and return the data."""
        logger.debug("Executing function %s with kwargs %r" % (function_key, kwargs))
        try:
            data = yield maybeDeferred(self.functions[function_key]['function'], **kwargs)
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

    def getPage(self, *args, **kwargs):
        return self.pg.getPage(*args, **kwargs)

    def setHostMaxRequestsPerSecond(self, *args, **kwargs):
        return self.rq.setHostMaxRequestsPerSecond(*args, **kwargs)

    def setHostMaxSimultaneousRequests(self, *args, **kwargs):
        return self.rq.setHostMaxSimultaneousRequests(*args, **kwargs)

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
        logger.debug("Got server data:\n%s" % PRETTYPRINTER.pformat(data))
        return data

    def setFastCache(self, uuid, data):
        if not isinstance(data, str):
            raise Exception("FastCache must be a string.")
        if uuid is None:
            return None
        self.fast_cache[uuid] = data

    def mapJob(self, job):
        if job.function_name in self.service_mapping:
            logger.debug('Remapping resource %s to %s' % (
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
                        logger.error('Could not find required argument %s for function %s in %s' % (
                            key, job.function_name, job))
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

class SmartConnectionPool(adbapi.ConnectionPool):
    def _runInteraction(self, *args, **kwargs):
        try:
            d = adbapi.ConnectionPool._runInteraction(self, *args, **kwargs)
        except OperationalError, e:
            errmsg = str(e).lower()
            messages = (
                "lost connection to mysql server during query",
                "server has gone away",
            )
            for msg in messages:
                if msg in errmsg:
                    return adbapi.ConnectionPool._runInteraction(self, *args, **kwargs)
            raise
        return d


