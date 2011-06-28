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
LOGGER = logging.getLogger(__name__)

from ..requestqueuer import RequestQueuer
from ..pagegetterlite import PageGetter
from ..resources import ExposedResource


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
        self.function_name = function_name
        self.kwargs = service_credentials
        self.subservice = function_name
        self.uuid = uuid
        self.user_account = user_account

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
        self._setupLogging(config["log_file"], config["log_directory"], config["log_level"])

    def _setupLogging(self, log_file, log_directory, log_level):
        if log_directory is None:
            self.logging_handler = logging.StreamHandler()
        else:
            self.logging_handler = logging.handlers.TimedRotatingFileHandler(
                os.path.join(log_directory, log_file),
                when='D',
                interval=1)
        log_format = "%(levelname)s: %(message)s %(pathname)s:%(lineno)d"
        self.logging_handler.setFormatter(logging.Formatter(log_format))
        LOGGER.addHandler(self.logging_handler)
        log_level = log_level.lower()
        log_levels = {
            "debug":logging.DEBUG,
            "info":logging.INFO,
            "warning":logging.WARNING,
            "error":logging.ERROR,
            "critical":logging.CRITICAL
        }
        if log_level in log_levels:
            LOGGER.setLevel(log_levels[log_level])
        else:
            LOGGER.setLevel(logging.DEBUG)

    def start(self):
        start_deferred = Deferred()
        reactor.callWhenRunning(self._baseStart, start_deferred)
        return start_deferred

    def _baseStart(self, start_deferred):
        LOGGER.debug("Starting Base components.")
        self.shutdown_trigger_id = reactor.addSystemEventTrigger(
            'before',
            'shutdown',
            self.shutdown)
        start_deferred.callback(True)

    @inlineCallbacks
    def shutdown(self):
        while self.rq.getPending() > 0 or self.rq.getActive() > 0:
            LOGGER.debug("%s requests active, %s requests pending." % (
                self.rq.getPending(),
                self.rq.getActive()
            ))
            shutdown_deferred = Deferred()
            # Call the Deferred after a second to continue the loop.
            reactor.callLater(1, shutdown_deferred.callback)
            yield shutdown_deferred
        self.shutdown_trigger_id = None
        LOGGER.critical("Server shut down.")
        LOGGER.removeHandler(self.logging_handler)
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
            raise
        # If the data is None, there's nothing to store.
        if job.uuid in self.active_jobs:
            del self.active_jobs[job.uuid]
        returnValue(data)

    @inlineCallbacks
    def executeFunction(self, function_key, **kwargs):
        """Execute a function by key w/ kwargs and return the data."""
        LOGGER.debug("Executing function %s with kwargs %r" % (function_key, kwargs))
        try:
            data = yield maybeDeferred(self.functions[function_key]['function'], **kwargs)
        except Exception, e:
            LOGGER.error("Error with %s.\n%s" % (function_key, e))
            raise e
        returnValue(data)

    def _getArguments(self, func):
        argspec = inspect.getargspec(func)
        # Get required / optional arguments
        arguments = argspec[0]
        kwarg_defaults = argspec[3]
        if len(arguments) > 0 and arguments[0:1][0] == 'self':
            arguments.pop(0)
        if kwarg_defaults is None:
            kwarg_defaults = []
        required_arguments = arguments[0:len(arguments) - len(kwarg_defaults)]
        optional_arguments = arguments[len(arguments) - len(kwarg_defaults):]
        return required_arguments, optional_arguments

    def makeCallable(self, func, interval=0, name=None, expose=False):
        required_arguments, optional_arguments = self._getArguments(func)
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
            "get_job_uuid":get_job_uuid,
            "delta":self.delta_functions.get(id(func), None)
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
        LOGGER.debug("Got server data:\n%s" % PRETTYPRINTER.pformat(data))
        return data

    def setFastCache(self, uuid, data):
        if not isinstance(data, str):
            raise Exception("FastCache must be a string.")
        if uuid is None:
            return None
        self.fast_cache[uuid] = data

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
            for key in f['required_arguments']:
                if key in mapping and mapping[key] in job.kwargs:
                    kwargs[key] = job.kwargs[mapping[key]]
                elif key in job.kwargs:
                    kwargs[key] = job.kwargs[key]
                else:
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


