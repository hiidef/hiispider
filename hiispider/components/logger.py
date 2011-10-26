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
        Component.__init__(self, server, server_mode)
        config = copy(config)
        conf = config.get('logd', {})
        conf.update(kwargs)
        if not conf or 'path' not in conf:
            raise Exception("Logger component requires configuration option `logd.path`.")
        self.path = conf['path']
        self.logd_port = conf.get('port', 8126)
        self.logd_host = conf.get('host', 'localhost')
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
    def closeOnError(self): return None

    @shared
    def send(self, s):
        self.sock.sendto(s, (self.logd_host, self.logd_port))

