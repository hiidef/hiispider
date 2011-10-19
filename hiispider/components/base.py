from twisted.internet.defer import Deferred, maybeDeferred, inlineCallbacks, returnValue
from twisted.internet import reactor
import logging


LOGGER = logging.getLogger(__name__)
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
        return maybeDeferred(func, self, *args, **kwargs)
    return decorator


class Component(object):
    """
    Abstract class that proxies component requests.
    """

    initialized = False
    server_mode = False
    running = False 
    requires = None

    def __init__(self, server, server_mode):
        if not self.requires:
            self.requires = []
        self.server = server
        self.server_mode = server_mode
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
        self.running = True
        if self.server_mode:
            yield maybeDeferred(self.start)       
        returnValue(None)

    def initialize(self):
        """Abstract initialization method."""
        pass
    
    def start(self):
        """Abstract start method."""
        pass

    @inlineCallbacks
    def _shutdown(self):
        self.running = False
        if self.server_mode:
            yield maybeDeferred(self.shutdown)
        returnValue(None)

    def shutdown(self):
        pass
