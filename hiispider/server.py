from components import Component
from txZMQ import ZmqFactory, ZmqEndpoint, ZmqEndpointType, ZmqConnection
from zmq.core.error import ZMQError
from zmq.core.constants import DEALER, ROUTER
from components import Queue, Logger, MySQL
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred
from twisted.internet import task
from cPickle import loads, dumps
import time

# Factory to make ZmqConnections
ZF = ZmqFactory() 
# Bind / Connect shortcuts
BIND, CONNECT = ZmqEndpointType.Bind, ZmqEndpointType.Connect
# The component class objects we intend to instantiate
COMPONENTS = [Queue, Logger, MySQL]
# The intra-server poll interval
POLL_INTERVAL = 5


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
                component = cls(config, address) # Instantiate as active
                self.active[name] = address # Keep track of actives
            else:
                component = cls(config) # Instantiate as inactive
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
                reactor.callLater(1, self._start2, data, start_deferred)
                return
        # Start the various components.
        d = DeferredList([maybeDeferred(x.start) for x in self.components])
        d.addCallback(self._start3, start_deferred)

    def _start3(self, data, start_deferred):
        print "Starting."
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

