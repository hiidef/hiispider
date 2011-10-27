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
        super(Logger, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        try:
            self.path = config['logd_path']
        except KeyError:
            raise Exception("Logger component requires "
                "configuration option `logd_path`.")
        self.logd_port = config.get('logd_port', 8126)
        self.logd_host = config.get('logd_host', 'localhost')
        self.logger = config.get('base_logger', logging.getLogger())
        self.sock = None
        logging.Handler.__init__(self)

    def initialize(self):
        self.sock = socket.UDPSocket(self.logd_host, self.logd_port)
        if self.__class__ not in (x.__class__ for x in self.logger.handlers):
            self.logger.addHandler(self)

    # make sure this stuff isn't done
    def makeSocket(self): 
        return None

    def createSocket(self): 
        return None

    def closeOnError(self):
        pass
        #TODO: What's ipdb?
        #ipdb.set_trace()

    @shared
    def send(self, s):
        self.sock.sendto(s, (self.logd_host, self.logd_port))
