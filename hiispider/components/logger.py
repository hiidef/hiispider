#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Communicates with Logd.
"""

from .base import Component, shared
from copy import copy
import logging
from pylogd.handlers import PylogdHandler
from pylogd.twisted import socket


LOGGER = logging.getLogger(__name__)


class Logger(Component, PylogdHandler):

    """Automatically attaches itself as a handler."""

    def __init__(self, server, config, server_mode, **kwargs):
        Component.__init__(self, server, server_mode)
        config = copy(config)
        conf = config.get('logd', {})
        conf.update(kwargs)
        self.sock = None
        if server_mode:
            if not conf or 'path' not in conf:
                raise Exception("Logger component requires "
                    "configuration option `logd.path`.")
            self.path = conf['path']
            self.logd_port = conf.get('port', 8126)
            self.logd_host = conf.get('host', 'localhost')
            self.logger = config.get('base_logger', logging.getLogger())
            logging.Handler.__init__(self)
            if self.__class__ not in (x.__class__ for x in self.logger.handlers):
                self.logger.addHandler(self)

    def initialize(self):
        self.sock = socket.UDPSocket(self.logd_host, self.logd_port)

    def makeSocket(self): 
        return None

    def createSocket(self): 
        return None

    def closeOnError(self):
        return None

    @shared
    def send(self, s):
        self.sock.sendto(s, (self.logd_host, self.logd_port))
