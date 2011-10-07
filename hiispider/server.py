from components import Component
from txZMQ import ZmqFactory, ZmqEndpoint, ZmqEndpointType, ZmqConnection
from zmq.core.error import ZMQError
from zmq.core.constants import ROUTER, DEALER
from components import Queue, Logger, MySQL
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred
from twisted.internet import task
from cPickle import loads, dumps
import time
from hiispider.components import *
from hiispider.metacomponents import *
import logging
from .resources import ExposedResource
import inspect

LOGGER = logging.getLogger(__name__)
# Factory to make ZmqConnections
ZF = ZmqFactory() 
# Bind / Connect shortcuts
BIND, CONNECT = ZmqEndpointType.Bind, ZmqEndpointType.Connect
# The component class objects we intend to instantiate
COMPONENTS = [
    Cassandra, 
    Logger, 
    MySQL,  
    Stats,
    Redis,
    JobQueue,
    PagecacheQueue, 
    IdentityQueue,
    PageGetter,
    Worker,
    JobHistoryRedis,
    JobGetter]
# The intra-server poll interval
POLL_INTERVAL = 5


from twisted.web.resource import Resource
import cStringIO, gzip
import traceback
import simplejson
import logging
import pprint

logger = logging.getLogger(__name__)

class ExposedFunctionResource(Resource):

    isLeaf = True

    def __init__(self):
        Resource.__init__(self)

    def _successResponse(self, data):
        # if isinstance(data, str):
        #      return data
        return simplejson.dumps(data)

    def _errorResponse(self, error):
        reason = str(error.value)
        # there are two ways to extract a traceback;  we pick whichever one
        # is longer as that probably has more information
        tbs = [error.getTraceback(),
               traceback.format_exc(traceback.extract_tb(error.tb))]
        tbs.sort(key=lambda x: len(x), reverse=True)
        tb = tbs[0]
        logger.error("%s\n%s" % (reason, tb))
        return simplejson.dumps({"error":reason, "traceback":tb})

    def _immediateResponse(self, data, request):
        # logger.debug("received data for request (%s):\n%s" % (request, pprint.pformat(simplejson.loads(data))))
        encoding = request.getHeader("accept-encoding")
        if encoding and "gzip" in encoding:
            zbuf = cStringIO.StringIO()
            zfile = gzip.GzipFile(None, 'wb', 9, zbuf)
            if isinstance(data, unicode):
                zfile.write(unicode(data).encode("utf-8"))
            elif isinstance(data, str):
                zfile.write(unicode(data, 'utf-8').encode("utf-8"))
            else:
                zfile.write(unicode(data).encode("utf-8"))
            zfile.close()
            request.setHeader("Content-encoding","gzip")
            request.write(zbuf.getvalue())
        else:
            request.write(data)
        request.finish()

    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        kwargs = {}
        for key in request.args:
            kwargs[key.replace('.', '_')] = request.args[key][0]
        d = maybeDeferred(self.primary_server.executeExposedFunction, self.function_name, **kwargs)
        d.addCallback(self._successResponse)
        d.addErrback(self._errorResponse)
        d.addCallback(self._immediateResponse, request)
        return server.NOT_DONE_YET


class ZmqCallbackConnection(ZmqConnection):
    """Adds a messageRecieved callback function."""

    def __init__(self, callback, *args, **kwargs):
        super(ZmqCallbackConnection, self).__init__(*args, **kwargs)
        self.callback = callback


class MetadataServer(ZmqCallbackConnection):
    """Reports metadata to connected clients."""

    socketType = ROUTER

    def messageReceived(self, message):
        # message is route, tag, optional dumped message
        message = message[0:2] + [loads(x) for x in message[2:]]
        self.callback(self, *message)

    def send(self, route, tag, message=None):
        if message:
            super(MetadataServer, self).send([route, tag, dumps(message)])
        else:
            super(MetadataServer, self).send([route, tag])


class MetadataClient(ZmqCallbackConnection):
    """Requests metadata from a connected server."""

    socketType = DEALER

    def messageReceived(self, message):
        # message is tag, optional dumped message
        message = message[0:1] + [loads(x) for x in message[1:]]
        self.callback(self, *message)

    def send(self, tag, message=None):
        if message:
            super(MetadataClient, self).send([tag, dumps(message)])
        else:
            super(MetadataClient, self).send([tag])

class Server(object):
    """
    Uses metadata servers and clients to tell components where to 
    look for peers.
    """

    connectionsloop = None
    components_start_deferred = None
    shutdown_trigger = None
    exposed_function_resources = {}
    function_resource = None
    functions = {}
    delta_functions = {}

    def __init__(self, config, address, *args):
        self.active = {} # name:address of active components
        self.inactive = {} # name:address of inactive components
        self.metadata_clients = {} # name:(client, last heartbeat timestamp)
        self.components = [] # Component objects
        ip, port = address.split(":")
        location = "tcp://%s:%s" % (ip, port)
        self.metadata_server = MetadataServer(
            self.metadata_server_callback, 
            ZF, 
            ZmqEndpoint(BIND, location))
        # Connect to servers in the config file.
        for address in config["servers"]:
            self.setup_client(address)
        for component in args:
            if not issubclass(component, Component):
                raise Exception("%s is not a Component" % component)
        # Loop through components, initializing them as active or inactive
        # Active components process requests, inactive components proxy
        # requests to other servers.
        for i, cls in enumerate(COMPONENTS):
            name = cls.__name__.lower()
            if cls in args:
                address = "%s:%s" % (ip, int(port) + 1 + i)
                component = cls(self, config, address) # Instantiate as active
                self.active[name] = address # Keep track of actives
            else:
                component = cls(self, config) # Instantiate as inactive
            self.components.append(component)
            setattr(self, name, component) # Attach component as property
        # Make sure we shut things down before the reactor stops.
        reactor.addSystemEventTrigger(
            'before',
            'shutdown',
            self.shutdown)
    
    def setup_client(self, address):
        """Make a connection to address."""
        if address in self.metadata_clients:
            self.metadata_clients[address][0].shutdown()
        client = MetadataClient(
            self.metadata_client_callback, 
            ZF, 
            ZmqEndpoint(CONNECT, "tcp://%s" % address))
        self.metadata_clients[address] = [client, time.time()]
        return client

    def metadata_server_callback(self, server, route, tag, message=None):
        """Responds to clients with metadata."""
        if tag == "server":
            # Message is COMPONENT_ADDRESS
            server.send(route, "server", (message, self.active))
        
    def metadata_client_callback(self, client, tag, message=None):
        """Handles server responses and updates heartbeat."""
        if tag == "server":
            # Message is COMPONENT_ADDRESS, list of COMPONENT_NAME
            self.metadata_clients[message[0]][1] = time.time()
            self.makeConnections(message[1])

    def makeConnections(self, data):
        # data is list of {COMPONENT_NAME:COMPONENT_ADDRESS}
        for name in data:
            getattr(self, name).makeConnection(data[name])

    def start(self):
        start_deferred = Deferred()
        reactor.callWhenRunning(self._start, start_deferred)
        return start_deferred
    
    def _start(self, start_deferred):
        # After one interval, attempt to communicate with the servers.
        # Important that you wait for a moment while the socket connects,
        # otherwise it breaks.
        self.connectionsloop = task.LoopingCall(self.getConnections)
        self.connectionsloop.start(POLL_INTERVAL, False)
        # Initialize components, but don't have them do anything yet.
        d = DeferredList([maybeDeferred(x.initialize) for x in self.components])
        d.addCallback(self._start2, start_deferred)

    def _start2(self, data, start_deferred):
        # Make sure the component is initialized or connected to a 
        # proxy component.
        for x in self.components:
            if not x.initialized:
                LOGGER.info("Waiting for %s" % x.__class__.__name__)
                reactor.callLater(1, self._start2, data, start_deferred)
                return
        # Start the various components.
        d = DeferredList([maybeDeferred(x.start) for x in self.components])
        d.addCallback(self._start3, start_deferred)

    def _start3(self, data, start_deferred):

        LOGGER.critical("Starting server with components: %s" % ", ".join(self.active.keys()))
        start_deferred.callback(True)

    def getConnections(self):
        for address in self.metadata_clients:
            client, timestamp = self.metadata_clients[address]
            # If it's been more than two poll intervals, attempt to reconnect.
            if time.time() - timestamp > POLL_INTERVAL * 2 + 1:
                self.setup_client(address)
                continue
            # Request a heartbeat.
            try:
                client.send("server", address)
            except ZMQError, e:
                print e

    def shutdown(self):
        if self.connectionsloop:
            self.connectionsloop.stop()
        self.metadata_server.shutdown()
        for client, timestamp in self.metadata_clients.values():
            client.shutdown()

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

    def delta(self, func, handler):
        self.delta_functions[id(func)] = handler


