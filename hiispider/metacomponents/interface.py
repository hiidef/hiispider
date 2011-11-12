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
        self.mysql = self.server.mysql  # For legacy plugins.

    def initialize(self):
        exposed = self.server.functions.items()
        for f in self.server.functions.values():
            f["pass_kwargs_to_callback"] = True
        for function_name, func in exposed:
            self.server.add_callback(function_name, self._execute_callback)
            self.server.add_errback(function_name, self._execute_errback)

        # disable fast cache on the interface server
        self.server.config['enable_fast_cache'] = False

    @inlineCallbacks
    def _execute_callback(self, data, kwargs):
        user_id = kwargs.get('site_user_id', '')
        if user_id:
            uuid = uuid4().hex
            yield self.server.cassandra.setData(user_id, data, uuid)
            returnValue({uuid: data})
        else:
            LOGGER.warn("Unable to save cassandra data for uuid %s kwargs %r" % (uuid, kwargs))
            returnValue(data)

    def _execute_errback(self, error):
        return error
