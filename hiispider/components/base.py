from txZMQ import ZmqFactory, ZmqEndpoint, ZmqEndpointType, ZmqConnection
from zmq.core.constants import DEALER, ROUTER
from twisted.internet import reactor
from twisted.internet.defer import Deferred, maybeDeferred, inlineCallbacks, returnValue
from cPickle import loads, dumps
from hiiguid import HiiGUID
import logging

from hiispider.exceptions import NotRunningException

LOGGER = logging.getLogger(__name__)

# Factory to make ZmqConnections

# Bind / Connect shortcuts
BIND, CONNECT = ZmqEndpointType.Bind, ZmqEndpointType.Connect
# Dictionary to hold deferreds. {tag:deferred}
DEFERRED_DICT = {}
BROADCASTED = []


def broadcasted(func):
    """
    Fanout Proxying decorator. If the component is in server_mode, field the
    request. If not, send the request to all members of the component pool.
    """
    BROADCASTED.append(func)
    def decorator(self, *args, **kwargs):
        LOGGER.critical("Broadcasted proxying not implemented.")
    return decorator


def shared(func):
    """
    Proxying decorator. If the component is in server_mode, field the
    request. If not, send the request to the component pool.
    """
    def decorator(self, *args, **kwargs):
        if self.server_mode:
            if not self._running:
                raise NotRunningException("%s not running. Could not "
                    "execute %s" % (self.__class__.__name__, func.__name__))
            return maybeDeferred(func, self, *args, **kwargs)
        else:
            return self.component_client.send(func.__name__, args, kwargs)
    return decorator


class ComponentServer(ZmqConnection):
    """
    Receives RPC requests from connected client components. Sends responses.
    """
    socketType = ROUTER

    def __init__(self, callback, *args, **kwargs):
        self.callback = callback
        super(ComponentServer, self).__init__(*args, **kwargs)

    def messageReceived(self, message):
        route = message[0]
        tag = message[1]
        LOGGER.debug("Received request %s via %s" % (HiiGUID(tag).base36, route))
        function_name, args, kwargs = loads(message[2])
        self.callback(route, tag, function_name, args, kwargs)

    def send(self, route, tag, message):
        LOGGER.debug("Responded to request %s via %s" % (HiiGUID(tag).base36, route))
        LOGGER.debug("%s pending." % len(DEFERRED_DICT))
        super(ComponentServer, self).send([route, tag, dumps(message)])


class ComponentClient(ZmqConnection):
    """
    Makes RPC requests to connected server components. Receives responses.
    """
    socketType = DEALER
    
    def messageReceived(self, message):
        LOGGER.debug("Received response %s" % HiiGUID(message[0]).base36)
        DEFERRED_DICT[message[0]].callback(loads(message[1]))
        del DEFERRED_DICT[message[0]]

    def send(self, function_name, args, kwargs):
        tag = HiiGUID().packed
        message = dumps([function_name, args, kwargs])
        LOGGER.debug("Sending request %s" % HiiGUID(tag).base36)
        super(ComponentClient, self).send([tag, message])
        d = Deferred()
        DEFERRED_DICT[tag] = d
        return d


class Component(object):
    """
    Abstract class that proxies component requests.
    """

    initialized = False
    component_client = None
    server_mode = False
    allow_clients = True # Allow other machines to connect
    _running = False 
    requires = None

    def __init__(self, server, address=None, allow_clients=None):
        ZF = ZmqFactory()
        if allow_clients is not None:
            self.allow_clients = allow_clients
        self.server = server
        self.connections = set([])
        self.active_connections = set([])
        if address:
            self.server_mode = True
            if self.allow_clients:
                LOGGER.info("Starting %s server at %s" % (self.__class__.__name__, address))
                # If in server mode, bind the socket.
                self.component_server = ComponentServer(
                    self._component_server_callback,
                    ZF, 
                    ZmqEndpoint(BIND, "tcp://%s" % address))
        # Shutdown before the reactor.
        reactor.addSystemEventTrigger(
            'before',
            'shutdown',
            self._shutdown)
        for func in BROADCASTED:
            self.server.expose(func)

    @inlineCallbacks
    def _initialize(self):
        if self.server_mode:
            yield maybeDeferred(self.initialize)
            self.initialized = True
        else:
            if self.__class__ not in self.server.requires:
                self.initialized = True
        returnValue(None)

    @inlineCallbacks
    def _start(self):
        """Abstract initialization method."""
        self._running = True
        if self.server_mode:
            yield maybeDeferred(self.start)       
        returnValue(None)

    def initialize(self):
        """Abstract initialization method."""
        pass
    
    def start(self):
        """Abstract start method."""
        pass

    def _component_server_callback(self, route, tag, function_name, args, kwargs):
        """
        Execute the requested call. Callback and errback are the same as we're 
        passing pickled objects back and forth.
        """
        d = getattr(self, function_name)(*args, **kwargs)
        d.addCallback(self._component_server_callback2, route, tag)
        d.addErrback(self._component_server_callback2, route, tag)

    def _component_server_callback2(self, message, route, tag):
        self.component_server.send(route, tag, message)

    def addConnection(self, address):
        self.connections.add(address)

    def makeConnections(self):
        """Connect to multiple remote servers."""
        if not self.server_mode:
            if len(self.connections - self.active_connections) > 0:
                ZF = ZmqFactory()
                self.active_connections.update(self.connections)
                if self.component_client:
                    self.component_client.shutdown()
                LOGGER.info("%s connecting to %s" % (self.__class__.__name__, ", ".join(self.active_connections)))              
                endpoints = [ZmqEndpoint(CONNECT, "tcp://%s" % x) for x in self.active_connections]
                self.component_client = ComponentClient(
                    ZF, 
                    *endpoints)
                self.initialized = True

    @inlineCallbacks
    def _shutdown(self):
        self._running = False
        if self.component_client:
            self.component_client.shutdown()
        if self.server_mode and self.allow_clients:
            self.component_server.shutdown()
        if self.server_mode:
            yield maybeDeferred(self.shutdown)
        returnValue(None)

    def shutdown(self):
        pass
