#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ..components.base import Component
from twisted.internet.defer import maybeDeferred, inlineCallbacks, returnValue
import logging

LOGGER = logging.getLogger(__name__)

class MetaComponent(Component):

    @inlineCallbacks
    def _shutdown(self):
        self.running = False
        if self.server_mode:
            yield maybeDeferred(self.shutdown)
        self.shutdown_complete = True
        returnValue(None)