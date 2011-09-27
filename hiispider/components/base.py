from txZMQ import ZmqConnection
from zmq.core.constants import REQ, REP
from twisted.internet import reactor
from twisted.internet.defer import Deferred



def shared(func):
    def decorator(self, *args, **kwargs):
        if self.server_mode:
            return func(self, *args, **kwargs)
        else:
            print "Get from remote place."
            return func(self, *args, **kwargs)
    return decorator


class ServerConnection(ZmqConnection):
    socket = REP


class ClientConnection(ZmqConnection):
    socket = REQ


class Component(object):

    server_mode = False

    def start(self, server_mode):
        self.server_mode = server_mode
        start_deferred = Deferred()
        reactor.callWhenRunning(self._start, start_deferred)
        return start_deferred

    def _start(self, start_deferred):
        raise NotImplementedError(self)