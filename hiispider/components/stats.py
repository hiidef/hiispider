#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Communicates with Logd.
"""

import logging
import msgpack
from random import random
from copy import copy
from pylogd.stats import Logd, Timer
from pylogd.twisted import socket
from .logger import Logger
from .base import Component, shared
from traceback import format_exc


LOGGER = logging.getLogger(__name__)


class Stats(Component, Logd):

    """Uses remote method _send to communicate with other servers."""

    def __init__(self, server, config, server_mode, **kwargs):
        super(Stats, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        self.logd_host = config.get('logd_host', 'localhost')
        self.logd_port = config.get('logd_port', 8126)
        self.addr = (self.logd_host, self.logd_port)
        self.prefix = config.get('logd_prefix', '')
        self.timer = Timer(self)

    def initialize(self):
        self.sock = socket.UDPSocket(self.logd_host, self.logd_port)

    def send(self, data, sample_rate=1):
        """Sends data to Logd via the remote _send method."""
        if sample_rate < 1:
            if random() > sample_rate:
                return
            data['rate'] = sample_rate
        if self.prefix:
            data['key'] = '%s:%s' % (self.prefix, data['key'])
        return self._send(data)

    @shared
    def _send(self, data):
        return self.sock.sendto(msgpack.dumps(data), self.addr)

