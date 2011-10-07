from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy

from pylogd.stats import Dummy, Logd
from pylogd.twisted import socket

import logging

logger = logging.getLogger(__name__)
Noop = Dummy

class Stats(Component, Logd):

    def __init__(self, server, config, address=None, **kwargs):
        super(Stats, self).__init__(server, address=address)
        config = copy(config)
        config.update(kwargs)
        self.logd_host = config.get('logd_host', 'localhost')
        self.logd_port = config.get('logd_port', 8126)
        self.addr = (self.logd_host, self.logd_port)
        self.prefix = config.get('logd_prefix', '')

    @inlineCallbacks
    def initialize(self):
        if self.server_mode:
            self.sock = socket.UDPSocket(self.logd_host, self.logd_port)

    def send(self, data, sample_rate=1):
        if sample_rate < 1:
            if random.random() > sample_rate:
                return
            data['rate'] = sample_rate
        if self.prefix:
            data['key'] = '%s:%s' % (self.prefix, data['key'])
        try:
            self._send(data)
        except:
            logger.error("unexpected error:\n%s" % traceback.format_exc())

    @shared
    def _send(self, data):
        return self.sock.sendto(msg, self.addr)

