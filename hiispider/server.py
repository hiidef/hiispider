#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Main server object. Connects to other servers via Perspective Broker."""


from cPickle import dumps
import logging
import inspect
import gzip
from io import StringIO
import simplejson
from requestqueuer import RequestQueuer
from collections import defaultdict
from traceback import format_exc
from twisted.web import server
from twisted.web.resource import Resource
from twisted.internet import reactor, task
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred, inlineCallbacks, returnValue
from twisted.spread import pb
from twisted.internet.error import ConnectionRefusedError
import twisted.spread.banana
from .components import *
from .metacomponents import *
from .sleep import Sleep


twisted.spread.banana.SIZE_LIMIT = 10 * 1024 * 1024
LOGGER = logging.getLogger(__name__)
# The component class objects we intend to instantiate
COMPONENTS = [
    Cassandra, 
    Logger, 
    MySQL,  
    Stats,
    Redis,
    JobQueue,
    PageCacheQueue, 
    IdentityQueue,
    PageGetter,
    Worker,
    JobHistoryRedis,
    JobGetter]
# The intra-server poll interval
POLL_INTERVAL = 60


def split_address(s):
    return s.split(":")[0], int(s.split(":")[1])


class ComponentServerException(Exception):
    pass


class ExposedFunctionResource(Resource):

    isLeaf = True

    def __init__(self, server, function_name):
        Resource.__init__(self)
        self._server = server
        self.function_name = function_name
    
    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        kwargs = {}
        for key in request.args:
            kwargs[key.replace('.', '_')] = request.args[key][0]
        f = self._server.functions[self.function_name]
        d = maybeDeferred(f["function"], **kwargs)
        d.addCallback(self._successResponse)
        d.addErrback(self._errorResponse)
        d.addCallback(self._immediateResponse, request)
        return server.NOT_DONE_YET

    def _successResponse(self, data):
        return simplejson.dumps(data)

    def _errorResponse(self, error):
        return simplejson.dumps({
            "error":str(error.value), 
            "traceback":error.getTraceback()})

    def _immediateResponse(self, data, request):
        encoding = request.getHeader("accept-encoding")
        if encoding and "gzip" in encoding:
            zbuf = StringIO()
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


class Server(pb.Root):
    """
    Holds components, makes simple HTTP requests, and manages PB connections.
    """

    connectionsloop = None
    components_start_deferred = None
    shutdown_trigger = None
    exposed_functions = []
    exposed_function_resources = {}
    function_resource = None
    functions = {}
    delta_functions = {}
    requires = set([])
    connections = {}
    connections_by_id = {}
    component_servers = defaultdict(dict)
    server_components = defaultdict(dict)
    components = [] # Component objects
    http = None
    pb = None
    getting_connections = False

    def __init__(self, 
            config, 
            address, 
            components=None, 
            provides=None, 
            http_port=None, 
            pb_port=None):
        if len(set(provides) - set(components)) > 0:
            raise ComponentServerException("Cannot provide a component not "
                "running in server mode.")
        self.provides = set([x.__name__.lower() for x in provides])
        self.address = address
        self.servers = config.get("servers", [])
        self.resource = Resource()
        # Connect to servers in the config file.
        for component in components:
            if not issubclass(component, Component):
                raise Exception("%s is not a Component" % component)
        # Loop through components, initializing them as active or inactive
        # Active components process requests, inactive components proxy
        # requests to other servers.
        for i, cls in enumerate(COMPONENTS):
            name = cls.__name__.lower()
            if cls in components:
                component = cls(self, config, True) # Instantiate as active
                self.requires.update([x.__name__.lower() for x in component.requires])
            else:
                component = cls(self, config, False) # Instantiate as inactive
            self.components.append(component)
            setattr(self, name, component) # Attach component as property
        # Set up connections 
        if http_port:
            self.http = reactor.listenTCP(http_port, server.Site(self.resource))
        if pb_port:
            self.pb = reactor.listenTCP(pb_port, pb.PBServerFactory(self))
        self.rq = RequestQueuer(
            max_simultaneous_requests=config["max_simultaneous_requests"],
            max_requests_per_host_per_second=config["max_requests_per_host_per_second"],
            max_simultaneous_requests_per_host=config["max_simultaneous_requests_per_host"])
        # Expose our internal methods.
        # Make sure we're not doing something weird like rate limiting local connections
        self.rq.setHostMaxRequestsPerSecond("127.0.0.1", 0)
        self.rq.setHostMaxSimultaneousRequests("127.0.0.1", 0)
        # Make sure we shut things down before the reactor stops.
        reactor.addSystemEventTrigger(
            'before',
            'shutdown',
            self.shutdown)

    def remote_components(self):
        return self.provides

    def start(self):
        start_deferred = Deferred()
        reactor.callWhenRunning(self._start, start_deferred)
        return start_deferred
    
    @inlineCallbacks
    def _start(self, start_deferred):
        # After one interval, attempt to communicate with the servers.
        # Important that you wait for a moment while the socket connects,
        # otherwise it breaks.
        self.connectionsloop = task.LoopingCall(self.getConnections)
        self.connectionsloop.start(POLL_INTERVAL)
        # Initialize components, but don't have them do anything yet.
        yield DeferredList([x._initialize() for x in self.components])
        # Make sure the component is initialized or connected to a 
        # proxy component.
        for x in self.components:
            while not x.initialized:
                LOGGER.info("Waiting for %s" % x.__class__.__name__)
                yield self.getConnections()
                yield Sleep(1)
            LOGGER.info("%s initialized." % x.__class__.__name__)
        yield Sleep(1)      
        active = ", ".join([x.__class__.__name__ 
            for x in self.components if x.server_mode])
        LOGGER.critical("Starting server with components: %s" % active)
        yield DeferredList([x._start() for x in self.components])
        start_deferred.callback(True)
    
    @inlineCallbacks
    def getConnections(self):
        if self.getting_connections:
            returnValue(None)
        self.getting_connections = True
        for server in self.servers:
            try:
                remote_obj = yield self.connect(server)
            except ConnectionRefusedError:
                LOGGER.error("Could not connect to %s" % server)
                for component in set(self.server_components[server]):
                    del self.server_components[server][component]
                    del self.component_servers[component][server]                    
                continue 
            try:
                components = yield remote_obj.callRemote("components")
            except:
                LOGGER.error(format_exc())
                continue
            for component in components - set(self.server_components[server]):
                if component in self.requires:
                    self.server_components[server][component] = remote_obj
                    self.component_servers[component][server] = remote_obj
                    getattr(self, component).initialized = True
            # depopulate server_components and component_servers
            for component in set(self.server_components[server]) - components:
                del self.server_components[server][component]
                del self.component_servers[component][server]
        for server in self.servers:
            if not self.server_components[server]:
                self.disconnect(server)
        self.getting_connections = False

    @inlineCallbacks       
    def connect(self, server):
        if server in self.connections:
            connection, remote_obj, factory = self.connections[server]
            returnValue(remote_obj)
        LOGGER.debug("Connecting to %s" % server)
        ip, port = split_address(server)
        factory = pb.PBClientFactory()
        connection = reactor.connectTCP(ip, port, factory)
        try:
            remote_obj = yield factory.getRootObject()
        except ConnectionRefusedError:
            connection.disconnect()
            raise
        remote_obj.notifyOnDisconnect(self.disconnect_by_remote_obj)
        self.connections_by_id[id(remote_obj)] = server
        self.connections[server] = (connection, remote_obj, factory)
        returnValue(remote_obj)

    def disconnect_by_remote_obj(self, remote_obj):
        if id(remote_obj) not in self.connections_by_id:
            return
        server = self.connections_by_id[id(remote_obj)]
        LOGGER.debug("%s disconnected." % server)
        self.disconnect(server)

    def disconnect(self, server):
        for component in set(self.server_components[server]):
            del self.server_components[server][component]
            del self.component_servers[component][server]
        if server not in self.connections:
            return
        LOGGER.debug("Disconnecting from %s" % server)
        connection, remote_obj, factory = self.connections[server]
        factory.disconnect()
        connection.disconnect()
        del self.connections_by_id[id(remote_obj)]
        del self.connections[server]

    @inlineCallbacks
    def shutdown(self):
        if self.connectionsloop:
            self.connectionsloop.stop()
        if self.http:
            self.http.stopListening()
        if self.pb:
            self.pb.stopListening()
        returnValue(None)

    def expose(self, *args, **kwargs):
        return self.makeCallable(expose=True, *args, **kwargs)

    def makeCallable(self, 
            func, 
            interval=0, 
            name=None, 
            expose=False, 
            category=None):
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
        #if function_name in self.functions:
        #    raise Exception("Function %s is already callable." % function_name)
        if function_name in self.functions:
            return
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
        #LOGGER.debug("Function %s is now callable." % function_name)
        if expose and self.resource is not None:
            self.exposed_functions.append(function_name)
            er = ExposedFunctionResource(self, function_name)
            function_name_parts = function_name.split("/")
            if len(function_name_parts) > 1:
                if function_name_parts[0] in self.exposed_function_resources:
                    r = self.exposed_function_resources[function_name_parts[0]]
                else:
                    r = Resource()
                    self.exposed_function_resources[function_name_parts[0]] = r
                self.resource.putChild(function_name_parts[0], r)
                r.putChild(function_name_parts[1], er)
            else:
                self.resource.putChild(function_name_parts[0], er)
            #LOGGER.info("%s is now available via HTTP." % function_name)
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



