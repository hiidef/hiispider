from txZMQ import ZmqFactory, ZmqEndpoint, ZmqEndpointType, ZmqConnection
from zmq.core.constants import DEALER, ROUTER
from twisted.internet import reactor
from twisted.internet.defer import Deferred, maybeDeferred, inlineCallbacks, returnValue
from cPickle import loads, dumps
from hiiguid import HiiGUID
import logging
from hiispider.exceptions import NotRunningException
from ..sleep import Sleep
from zmq.core import constants
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

from random import random
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


from base64 import b64encode



class ComponentServer(ZmqConnection):
    """
    Receives RPC requests from connected client components. Sends responses.
    """
    socketType = ROUTER
    server_req_count = 0
    server_res_count = 0
    balance = 0

    def __init__(self, identity, callback, *args, **kwargs):
        self.identity = identity
        self.callback = callback
        super(ComponentServer, self).__init__(*args, **kwargs)

    def messageReceived(self, message):
        route = message[0]
        tag, function_name, args, kwargs = loads(message[1])
        self.server_req_count += 1
        self.balance -= 1
        LOGGER.debug("Received %s request #%s (%s) %s via %s" % (self.identity, self.server_req_count, self.balance, HiiGUID(tag).base36, b64encode(route)))
        self.callback(route, tag, function_name, args, kwargs)

    def send(self, route, tag, data):
        self.balance += 1
        self.server_res_count += 1
        LOGGER.debug("Responded to %s request #%s (%s) %s via %s" % (self.identity, self.server_res_count, self.balance, HiiGUID(tag).base36, b64encode(route)))
        LOGGER.debug("%s pending." % len(DEFERRED_DICT))
        super(ComponentServer, self).send([route, dumps((tag, data))])


class ComponentClient(ZmqConnection):
    """
    Makes RPC requests to connected server components. Receives responses.
    """
    socketType = DEALER
    client_req_count = 0
    client_res_count = 0
    balance = 0

    def __init__(self, identity, *args, **kwargs):
        self.queue = []
        self.identity = identity
        super(ComponentClient, self).__init__(*args, **kwargs)
    
    def messageReceived(self, message):
        tag, data = loads(message[0])
        self.balance -= 1
        self.client_res_count += 1
        LOGGER.debug("Received %s response #%s (%s) %s" % (self.identity, self.client_res_count, self.balance, HiiGUID(tag).base36))
        DEFERRED_DICT[tag].callback(data)
        del DEFERRED_DICT[tag]
        self._send()

    def send(self, function_name, args, kwargs):
        tag = HiiGUID().packed
        message = dumps((tag, function_name, args, kwargs))
        d = Deferred()
        DEFERRED_DICT[tag] = d
        self.queue.append(message)
        self._send()
        if self.balance > 5:
            self.socket.getsockopt(constants.EVENTS)
        return d
    
    def _send(self):
        while self.queue:
            message = self.queue.pop()
            self.balance += 1
            self.client_req_count += 1
            tag, function_name, args, kwargs = loads(message)
            LOGGER.debug("Sending %s request #%s (%s) %s" % (self.identity, self.client_req_count, self.balance, HiiGUID(tag).base36))
            try:
                super(ComponentClient, self).send([message])
            except Exception, e:
                LOGGER.error(e)
                DEFERRED_DICT[tag].errback(e)
                del DEFERRED_DICT[tag]
#class ComponentClient(ZmqConnection):
#    """
#    Makes RPC requests to connected server components. Receives responses.
#    """
#    socketType = DEALER
#    client_req_count = 0
#    client_res_count = 0
#    balance = 0
#
#    def __init__(self, identity, *args, **kwargs):
#        self.identity = identity
#        super(ComponentClient, self).__init__(*args, **kwargs)
#    
#    def messageReceived(self, message):
#        self.balance -= 1
#        self.client_res_count += 1
#        LOGGER.debug("Received %s response #%s (%s) %s" % (self.identity, self.client_res_count, self.balance, HiiGUID(message[0]).base36))
#        DEFERRED_DICT[message[0]].callback(loads(message[1]))
#        del DEFERRED_DICT[message[0]]
#
#    def send(self, function_name, args, kwargs):
#        self.balance += 1
#        self.client_req_count += 1
#        tag = HiiGUID().packed
#        message = dumps([function_name, args, kwargs])
#        LOGGER.debug("Sending %s request #%s (%s) %s" % (self.identity, self.client_req_count, self.balance, HiiGUID(tag).base36))
#        super(ComponentClient, self).send([tag, message])
#        d = Deferred()
#        DEFERRED_DICT[tag] = d
#        return d


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
                    "%s.%s.server" % (self.server.address, self.__class__.__name__),
                    self._component_server_callback,
                    self.server.ZF, 
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
    
    @inlineCallbacks
    def reconnect(self):
        if self.component_client:
            LOGGER.info("Shutting down component client before reconnect.")
            self.component_client.shutdown()
            yield Sleep(1)
        LOGGER.info("%s connecting to %s" % (self.__class__.__name__, ", ".join(self.active_connections)))              
        endpoints = [ZmqEndpoint(CONNECT, "tcp://%s" % x) for x in self.active_connections]
        self.component_client = ComponentClient(
            "%s.%s.client" % (self.server.address, self.__class__.__name__),
            self.server.ZF, 
            *endpoints)
        yield Sleep(1)      

    @inlineCallbacks
    def makeConnections(self):
        """Connect to multiple remote servers."""
        if not self.server_mode:
            if len(self.connections - self.active_connections) > 0:
                self.active_connections.update(self.connections)
                yield self.reconnect()
                self.initialized = True
        returnValue(None)

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
