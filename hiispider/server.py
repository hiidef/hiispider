from components import Component
from components import Queue, Logger, MySQL
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred, inlineCallbacks
from twisted.internet import task
from cPickle import loads, dumps
import time
from hiispider.components import *
from hiispider.metacomponents import *
import logging
from .resources import ExposedResource
import inspect
from twisted.web import server
from twisted.web.resource import Resource
import gzip
from io import StringIO
import simplejson
from traceback import format_tb, format_exc
from requestqueuer import RequestQueuer
from collections import defaultdict
from sleep import Sleep
from twisted.spread import pb

LOGGER = logging.getLogger(__name__)
# Factory to make ZmqConnections


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
        # logger.debug("received data for request (%s):\n%s" % (request, pprint.pformat(simplejson.loads(data))))
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
    Uses metadata servers and clients to tell components where to 
    look for peers.
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
    connected_servers = {}
    connected_server_factories = {}
    component_servers = defaultdict(dict)
    server_components = defaultdict(dict)
    components = [] # Component objects

    def __init__(self, config, address, components=None, provides=None):
        if len(set(provides) - set(components)) > 0:
            raise ComponentServerException("Cannot provide a component not "
                "running in server mode.")
        self.provides = [x.__name__.lower() for x in provides]
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
        ip, port = split_address(address)
        self.http_port = reactor.listenTCP(port, server.Site(self.resource))
        self.rq = RequestQueuer(
            max_simultaneous_requests=config["max_simultaneous_requests"],
            max_requests_per_host_per_second=config["max_requests_per_host_per_second"],
            max_simultaneous_requests_per_host=config["max_simultaneous_requests_per_host"])
        self.pb_port = reactor.listenTCP(port + 1, pb.PBServerFactory(self))
        # Expose our internal methods.
        self.expose(self.availableComponents)
        # Make sure we're not doing something weird like rate limiting local connections
        self.rq.setHostMaxRequestsPerSecond("127.0.0.1", 0)
        self.rq.setHostMaxSimultaneousRequests("127.0.0.1", 0)
        # Make sure we shut things down before the reactor stops.
        reactor.addSystemEventTrigger(
            'before',
            'shutdown',
            self.shutdown)

    def availableComponents(self):
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
                yield Sleep(1)
        yield Sleep(1)      
        active = ", ".join([x.__class__.__name__ for x in self.components if x.server_mode])
        LOGGER.critical("Starting server with components: %s" % active)
        yield DeferredList([x._start() for x in self.components])
        start_deferred.callback(True)
    
    @inlineCallbacks
    def getConnections(self):
        for server in self.servers:
            try:
                url = "http://%s/server/availablecomponents" % server
                data = yield self.rq.getPage(url)
            except:
                LOGGER.error("%s could not be "
                    "contacted: %s" % (server, format_exc()))
                for component in set(self.server_components[server]):
                    del self.component_servers[component][server]
                self.server_components[server] = {}                    
                continue
            components = set(simplejson.loads(data["response"]))
            for component in components - set(self.server_components[server]):
                if component in self.requires:
                    try:
                        connection = yield self.connect(server)
                        self.server_components[server][component] = connection
                        self.component_servers[component][server] = connection
                    except:
                        LOGGER.error("%s could not be "    
                            "connected: %s" % (server, format_exc()))                        
            # depopulate server_components and component_servers
            for component in set(self.server_components[server]) - components:
                del self.server_components[server][component]
                del self.component_servers[component][server]
            if not self.server_components[server]:
                self.disconnect(server)

    @inlineCallbacks       
    def connect(self, server):
        if server in self.connected_servers:
            returnValue(self.connected_servers[server])
        ip, port = split_address(server)
        factory = pb.PBClientFactory()
        reactor.connectTCP(ip, port + 1, factory)
        remote = yield factory.getRootObject()
        self.connected_servers[server] = remote
        self.connected_server_factories[server] = factory
        returnValue(remote)

    def disconnect(self, server):
        if server in self.connected_servers:
            self.connected_server_factories[server].disconnect()
        del self.connected_servers[server]

    def shutdown(self):
        if self.connectionsloop:
            self.connectionsloop.stop()
        self.http_port.stopListening()
        self.pb_port.stopListening()

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
        LOGGER.info("Function %s is now callable." % function_name)
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


