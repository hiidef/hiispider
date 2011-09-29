from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy


class MySQL(Component):

    def __init__(self, config, address=None, **kwargs):
        super(MySQL, self).__init__(address=address)
        config = copy(config)
        config.update(kwargs)
    
    @shared
    def marco(self, s):
        return "Polo: %s" % s
        