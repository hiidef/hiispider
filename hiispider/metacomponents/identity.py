#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Abstract class for identity methods."""

import logging
import time
from .base import MetaComponent
from copy import copy
from hiispider.components import *
from hiispider.metacomponents import PageGetter
from traceback import format_exc, format_tb
from twisted.internet import task, reactor
from collections import defaultdict
from twisted.internet.defer import inlineCallbacks, maybeDeferred
from pprint import pformat


LOGGER = logging.getLogger(__name__)


def invert(d):
    """Invert a dictionary."""
    return dict([(v, k) for (k, v) in d.iteritems()])


class Identity(MetaComponent):

    requires = [Logger, MySQL, Cassandra, PageGetter]

    def __init__(self, server, config, server_mode, **kwargs):
        super(Identity, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        self.plugin_mapping = config["plugin_mapping"]
        self.service_args_mapping = config["service_args_mapping"]
        self.inverted_args_mapping = dict([(s[0], invert(s[1]))
            for s in self.service_args_mapping.items()])

    def get_service_connections(self, user):    
        service = self.plugin_mapping.get(
            user["_account_type"], 
            user["_account_type"])
        function_key = "%s/_getconnections" % service
        f = self.server.functions[function_key]
        d = maybeDeferred(f["function"], **user)
        d.addCallback(self._get_service_connections_callback, service, user)
        d.addErrback(self._get_service_connections_errback)
        return d

    @inlineCallbacks
    def _get_service_connections_callback(self, ids, service, user):
        ids = set(ids)
        # Currently stored connections
        current = yield self.server.cassandra.getServiceConnections(
            service, 
            user["user_id"])
        # Remove Currently stored connections no longer in the service
        yield self.server.cassandra.removeConnections(
            service, 
            user["user_id"], 
            dict([x for x in current.items() if x[0] in set(current) - ids]))
        # Add connections in the service not currently stored
        yield self.server.cassandra.addConnections(
            service, 
            user["user_id"],
            ids - set(current))

    def _get_service_connections_errback(self, error):
        try:
            error.raiseException()
        except NotImplementedError:
            return
        except Exception, e:
            tb = '\n'.join(format_tb(error.getTracebackObject()))
            LOGGER.error("Error getting service connections: %s\n%s" % (
                tb,
                format_exc()))

    def get_service_identity(self, user):
        service = self.plugin_mapping.get(
            user["_account_type"], 
            user["_account_type"])
        function_key = "%s/_getidentity" % service
        f = self.server.functions[function_key]
        d = maybeDeferred(f["function"], **user)
        d.addCallback(self._get_service_identity_callback, service, user)
        d.addErrback(self._get_service_identity_errback)
        return d

    @inlineCallbacks
    def _get_service_identity_callback(self, service_id, service, user):
        yield self.server.cassandra.setServiceIdentity(
            service, 
            user["user_id"], 
            service_id) 

    def _get_service_identity_errback(self, error):
        try:
            error.raiseException()
        except NotImplementedError:
            return
        except Exception, e:
            tb = '\n'.join(format_tb(error.getTracebackObject()))
            LOGGER.error("Error getting identity connections: %s\n%s" % (
                tb,
                format_exc()))

        