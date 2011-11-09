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
        try:
            for f in self.server.functions:
                f["pass_kwargs_to_callback"] = True
        except:
            import traceback
            traceback.print_exc()
        for function_name, func in exposed:
            self.server.add_callback(function_name, self._execute_callback)
            self.server.add_errback(function_name, self._execute_errback)
            LOGGER.debug("Added %s callback and errback." % function_name)

    @inlineCallbacks
    def _execute_callback(self, data, kwargs):
        uuid = uuid4().hex
        # FIXME: what should we do if there's no site_user_id?
        user_id = kwargs.get('site_user_id', '')
        yield self.server.cassandra.setData(data, uuid)
        returnValue({uuid:data})

    def _execute_errback(self, error):
        return error

