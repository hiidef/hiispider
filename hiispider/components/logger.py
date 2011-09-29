from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy
from twisted.internet import task
import time

class Logger(Component):

    starttime = time.time()
    fooloop = None
    cycles = 0

    def __init__(self, config, address=None, **kwargs):
        super(Logger, self).__init__(address=address)
        config = copy(config)
        config.update(kwargs)

    def start(self):
        self.fooloop = task.LoopingCall(self.foo)
        self.fooloop.start(10)
        if not self.server_mode:
            print "Not in server mode."
            self.foocycle()

    def shutdown(self):
        if self.fooloop:
            self.fooloop.stop()

    def foo(self):
        print "%s cycles per second" % (self.cycles / (time.time() - self.starttime))

    @inlineCallbacks
    def foocycle(self):
        while True:
            yield self._foo()
            self.cycles += 1

    @shared
    def _foo(self):
        return True
        