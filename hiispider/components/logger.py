from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy
from twisted.internet import task
import time
import logging

from pylogd.handlers import PylogdHandler
from pylogd.twisted import socket


logger = logging.getLogger(__name__)

class Logger(Component, PylogdHandler):

    def __init__(self, server, config, server_mode, **kwargs):
        super(Logger, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        try:
            self.path = config['logd_path']
        except KeyError:
            raise Exception("Logger component requires configuration option `logd_path`.")
        self.logd_port = config.get('logd_port', 8126)
        self.logd_host = config.get('logd_host', 'localhost')
        self.logger = config.get('base_logger', logging.getLogger())
        self.sock = None
        logging.Handler.__init__(self)
        if self.__class__ not in (x.__class__ for x in self.logger.handlers):
            self.logger.addHandler(self)

    def initialize(self):
        self.sock = socket.UDPSocket(self.logd_host, self.logd_port)

    # make sure this stuff isn't done
    def makeSocket(self): return None
    def createSocket(self): return None
    def closeOnError(self):
        import ipdb; ipdb.set_trace();

    @shared
    def send(self, s):
        self.sock.sendto(s, (self.logd_host, self.logd_port))

