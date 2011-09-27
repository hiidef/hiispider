
from components import Queue, Logger, Component
from twisted.internet import reactor
from twisted.internet.defer import DeferredList
from twisted.internet import task

class Server(object):

    components = []

    def __init__(self, config, port, *args):
        for component in args:
            if not issubclass(component, Component):
                raise Exception("%s is not a Component" % component)
        self.components = args
        self.queue = Queue(config)
        self.logger = Logger(config)

    def start(self):
        d = DeferredList([
            self.queue.start(Queue in self.components),
            self.logger.start(Logger in self.components)])
        self.testloop = task.LoopingCall(self.test)
        self.testloop.start(5)
        return d

    def test(self):
        print self.logger.foo("Hi")
