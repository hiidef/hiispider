from twisted.internet.defer import Deferred
from twisted.internet import reactor

class Sleep(Deferred):
    def __init__(self, timeout):
        Deferred.__init__(self)
        reactor.callLater(timeout, self.callback, None)