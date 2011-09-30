from txZMQ import ZmqFactory, ZmqEndpoint, ZmqEndpointType, ZmqConnection
from zmq.core.constants import DEALER, ROUTER
from twisted.internet import reactor
from twisted.internet.defer import Deferred, maybeDeferred
from cPickle import loads, dumps
from hiiguid import HiiGUID
import logging

LOGGER = logging.getLogger(__name__)

# Factory to make ZmqConnections
ZF = ZmqFactory()
# Bind / Connect shortcuts
BIND, CONNECT = ZmqEndpointType.Bind, ZmqEndpointType.Connect
# Dictionary to hold deferreds. {tag:deferred}
DEFERRED_DICT = {}


def broadcasted(func):
    """
    Fanout Proxying decorator. If the component is in server_mode, field the
    request. If not, send the request to all members of the component pool.
    """
    def decorator(self, *args, **kwargs):
        LOGGER.critical("Broadcasted proxying not implemented.")
        if self.server_mode:
            return maybeDeferred(func, self, *args, **kwargs)
        else:
            return self.component_client.send(func.__name__, args, kwargs)
    return decorator


def shared(func):
    """
    Proxying decorator. If the component is in server_mode, field the
    request. If not, send the request to the component pool.
    """
    def decorator(self, *args, **kwargs):
        if self.server_mode:
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
        function_name, args, kwargs = loads(message[2])
        self.callback(route, tag, function_name, args, kwargs)

    def send(self, route, tag, message):
        super(ComponentServer, self).send([route, tag, dumps(message)])


class ComponentClient(ZmqConnection):
    """
    Makes RPC requests to connected server components. Receives responses.
    """
    socketType = DEALER
    
    def messageReceived(self, message):
        DEFERRED_DICT[message[0]].callback(loads(message[1]))
        del DEFERRED_DICT[message[0]]

    def send(self, function_name, args, kwargs):
        tag = HiiGUID().packed
        message = dumps([function_name, args, kwargs])
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
    connected = True # Connect to other servers of this type.

    def __init__(self, server, address=None):
        self.server = server
        self.connections = []
        if address and self.connected:
            ip, port = address.split(":")
            self.server_mode = True
            # If in server mode, bind the socket.
            self.component_server = ComponentServer(
                self._component_server_callback,
                ZF, 
                ZmqEndpoint(BIND, "tcp://%s:%s" % (ip, port)))
        # Shutdown before the reactor.
        reactor.addSystemEventTrigger(
            'before',
            'shutdown',
            self._shutdown)

    def initialize(self):
        """Abstract initialization method."""
        if self.server_mode:
            self.initialized = True

    def start(self):
        """Abstract initialization method."""
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

    def makeConnection(self, address):
        """Connect to multiple remote servers."""
        if not self.connected:
            self.initialized = True
            return
        if address not in self.connections:
            if self.component_client:
                self.component_client.shutdown()
            LOGGER.info("Connecting to %s" % address)
            self.connections.append(address)
            endpoints = [ZmqEndpoint(CONNECT, "tcp://%s" % x) for x in self.connections]
            self.component_client = ComponentClient(
                ZF, 
                *endpoints)
            if not self.server_mode:
                self.initialized = True

    def _shutdown(self):
        if self.component_client:
            self.component_client.shutdown()
        if self.server_mode and self.connected:
            self.component_server.shutdown()
        return self.shutdown()

    def shutdown(self):
        pass
