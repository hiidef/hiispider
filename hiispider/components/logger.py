from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy
from twisted.internet import task
import time
import logging


LOGGER = logging.getLogger(__name__)


class Logger(Component):

    def __init__(self, server, config, address=None, **kwargs):
        super(Logger, self).__init__(server, address=address)
        config = copy(config)
        config.update(kwargs)
