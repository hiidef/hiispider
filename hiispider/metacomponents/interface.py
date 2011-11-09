#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Executes new jobs on behalf of the Django application."""

from uuid import uuid4
from twisted.internet.defer import inlineCallbacks, returnValue
from hiispider.components import *
from hiispider.metacomponents import *
import logging


LOGGER = logging.getLogger(__name__)


class Interface(Component):

    allow_clients = False
    requires = [MySQL, PageGetter, Cassandra, Logger]

    def __init__(self, server, config, server_mode, **kwargs):
        super(Interface, self).__init__(server, server_mode)
        self.mysql = self.server.mysql # For legacy plugins.

    def initialize(self):
        exposed = [x for x in self.server.functions.items() if x[1]["interval"] > 0]
        for f in self.server.functions.values():
            f["pass_kwargs_to_callback"] = True
        for function_name, func in exposed:
            self.server.add_callback(function_name, self._execute_callback)
            self.server.add_errback(function_name, self._execute_errback)
            LOGGER.debug("Added %s callback and errback." % function_name)

        # disable fast cache on the interface server
        self.server.config['enable_fast_cache'] = False

    @inlineCallbacks
    def _execute_callback(self, data, kwargs):
        uuid = uuid4().hex
        # FIXME: what should we do if there's no site_user_id?
        user_id = kwargs.get('site_user_id', '')
        yield self.server.cassandra.setData(user_id, data, uuid)
        returnValue({uuid:data})

    def _execute_errback(self, error):
        return error

