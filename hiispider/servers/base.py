import inspect
import logging
import logging.handlers
import os
import time
import pprint
from decimal import Decimal
from uuid import uuid4
from twisted.web.resource import Resource
from twisted.internet import reactor
from twisted.internet.threads import deferToThread
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred
from ..requestqueuer import RequestQueuer
from ..resources import ExposedResource

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)

LOGGER = logging.getLogger("main")

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
    reservation_fast_caches = {}
    function_resource = None
    
    def __init__(self,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 max_simultaneous_requests=100,
                 max_requests_per_host_per_second=0,
                 max_simultaneous_requests_per_host=0,
                 log_file=None,
                 log_directory=None,
                 log_level="debug",
                 port=8080,
                 pg=None):
        self.start_deferred = Deferred()
        self.rq = RequestQueuer( 
            max_simultaneous_requests=int(max_simultaneous_requests), 
            max_requests_per_host_per_second=int(max_requests_per_host_per_second), 
            max_simultaneous_requests_per_host=int(max_simultaneous_requests_per_host))
        self.rq.setHostMaxRequestsPerSecond("127.0.0.1", 0)
        self.rq.setHostMaxSimultaneousRequests("127.0.0.1", 0)
        self.aws_access_key_id=aws_access_key_id
        self.aws_secret_access_key=aws_secret_access_key
        if pg is None:
            self.pg = self.rq
        else:
            self.pg = pg
        self._setupLogging(log_file, log_directory, log_level)

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
                reactor.callWhenRunning(self._start)
                return self.start_deferred

    def start(self):
        reactor.callWhenRunning(self._baseStart)
        return self.start_deferred
    
    def _baseStart(self):
        self.shutdown_trigger_id = reactor.addSystemEventTrigger(
            'before', 
            'shutdown', 
            self.shutdown)
        LOGGER.critical("Starting.")
        self._baseStartCallback2(None)

    def _baseStartCallback2(self, data):
        self.start_deferred.callback(True)

    def _startHandleError(self, data, error):
        self.start_deferred.errback(error)
        
    def shutdown(self):
        LOGGER.debug("Waiting for shutdown.")
        d = Deferred()
        reactor.callLater(0, self._waitForShutdown, d)
        return d

    def _waitForShutdown(self, shutdown_deferred):          
        if self.rq.getPending() > 0 or self.rq.getActive() > 0:
            LOGGER.debug("Waiting for shutdown.")
            reactor.callLater(1, self._waitForShutdown, shutdown_deferred)
            return
        self.shutdown_trigger_id = None
        LOGGER.debug("Shut down.")
        LOGGER.removeHandler(self.logging_handler)
        shutdown_deferred.callback(True)
            
    def expose(self, *args, **kwargs):
        return self.makeCallable(expose=True, *args, **kwargs)

    def makeCallable(self, func, interval=0, name=None, expose=False):
        argspec = inspect.getargspec(func)
        # Get required / optional arguments
        arguments = argspec[0]
        if len(arguments) > 0 and arguments[0:1][0] == 'self':
            arguments.pop(0)
        kwarg_defaults = argspec[3]
        if kwarg_defaults is None:
            kwarg_defaults = []
        required_arguments = arguments[0:len(arguments) - len(kwarg_defaults)]
        optional_arguments = arguments[len(arguments) - len(kwarg_defaults):]
        # Reservation fast cache is stored on with the reservation
        if "reservation_fast_cache" in required_arguments:
            del required_arguments[required_arguments.index("reservation_fast_cache")]
            check_reservation_fast_cache = True
        elif "reservation_fast_cache" in optional_arguments:
            del optional_arguments[optional_arguments.index("reservation_fast_cache")]
            check_reservation_fast_cache = True
        else:
            check_reservation_fast_cache = False
        # Indicates whether to send the reservation's UUID to the function
        if "reservation_uuid" in required_arguments:
            del required_arguments[required_arguments.index("reservation_uuid")]
            get_reservation_uuid = True
        elif "reservation_uuid" in optional_arguments:
            del optional_arguments[optional_arguments.index("reservation_uuid")]
            get_reservation_uuid = True
        else:
            get_reservation_uuid = False
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
                message = "Required argument name '%s' used in function %s is reserved." % (key, function_name)
                LOGGER.error(message)
                raise Exception(message)
        for key in optional_arguments:
            if key in self.reserved_arguments:
                message = "Optional argument name '%s' used in function %s is reserved." % (key, function_name)
                LOGGER.error(message)
                raise Exception(message)
        # Make sure we don't already have a function with the same name.
        if function_name in self.functions:
            raise Exception("A method or function with the name %s is already callable." % function_name)
        # Add it to our list of callable functions.
        self.functions[function_name] = {
            "function":func,
            "interval":interval,
            "required_arguments":required_arguments,
            "optional_arguments":optional_arguments,
            "check_reservation_fast_cache":check_reservation_fast_cache,
            "get_reservation_uuid":get_reservation_uuid
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
            LOGGER.info("Function %s is now available via the HTTP interface." % function_name)
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

    def setReservationFastCache(self, uuid, data):
        if not isinstance(data, str):
            raise Exception("ReservationFastCache must be a string.")
        if uuid is None:
            return None
        self.reservation_fast_caches[uuid] = data


    def callExposedFunction(self, func, kwargs, function_name, reservation_fast_cache=None, uuid=None):
        if uuid is not None:
            self.active_jobs[uuid] = True
        if self.functions[function_name]["get_reservation_uuid"]:
            kwargs["reservation_uuid"] = uuid 
        if self.functions[function_name]["check_reservation_fast_cache"] and \
                reservation_fast_cache is not None:
            kwargs["reservation_fast_cache"] = reservation_fast_cache
        elif self.functions[function_name]["check_reservation_fast_cache"]:
            kwargs["reservation_fast_cache"] = None
        d = maybeDeferred(func, **kwargs)
        d.addCallback(self._callExposedFunctionCallback, function_name, uuid)
        d.addErrback(self._callExposedFunctionErrback, function_name, uuid)
        return d

    def _callExposedFunctionErrback(self, error, function_name, uuid):
        if uuid is not None and uuid in self.active_jobs:
            del self.active_jobs[uuid]
        if uuid is None:
            LOGGER.error("Error with %s.\n%s" % (function_name, error))
        else:
            LOGGER.error("Error with %s.\nUUID:%s\n%s" % (
                function_name, 
                uuid,
                error))
        return error

    def _callExposedFunctionCallback(self, data, function_name, uuid):
        LOGGER.debug("Function %s returned successfully." % (function_name))
        # If the UUID is None, this is a one-off type of thing.
        if uuid is None:
            return data
        # If the data is None, there's nothing to store.
        if data is None:
            del self.active_jobs[uuid]
            return None
        if uuid in self.active_jobs:
            del self.active_jobs[uuid]
        return data



