from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy

from pylogd.stats import Logd, Timer
from pylogd.twisted import socket
from random import random
import logging
import msgpack
from logger import Logger


LOGGER = logging.getLogger(__name__)


class Stats(Component, Logd):

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
        if sample_rate < 1:
            if random() > sample_rate:
                return
            data['rate'] = sample_rate
        if self.prefix:
            data['key'] = '%s:%s' % (self.prefix, data['key'])
        try:
            self._send(data)
        except:
            LOGGER.error("unexpected error:\n%s" % traceback.format_exc())

    @shared
    def _send(self, data):
        return self.sock.sendto(msgpack.dumps(data), self.addr)

