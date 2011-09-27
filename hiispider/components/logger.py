from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy


class Logger(Component):

    def __init__(self, config, **kwargs):
        config = copy(config)
        config.update(kwargs)

    def _start(self, start_deferred):
        start_deferred.callback("Logger started successfully.")
    
    @shared
    def foo(self, s):
        return "Bar: %s" % s
        