from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy
import logging


LOGGER = logging.getLogger(__name__)


class Noop(object):
    def __getattr__(self, attr):
        return self.noop

    def noop(self, *args, **kwargs):
        return


class Stats(Component):

    def __init__(self, server, config, address=None, **kwargs):
        super(Stats, self).__init__(server, address=address)
        config = copy(config)
        config.update(kwargs)
        self.timer = Noop()

    def timed(self, *args, **kwargs):
        def decorator(f):
            return f
        return decorator
