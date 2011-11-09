#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Base component module. Implements decorators used for sharing and broadcasting
methods via Perspective broker between the components.
"""

import time
import logging
import inspect
import types
from random import choice
from cPickle import dumps, loads
from twisted.spread import pb
from twisted.internet.defer import Deferred, maybeDeferred
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from hiispider.exceptions import ComponentException


LOGGER = logging.getLogger(__name__)
SHARED = {}
INVERSE_SHARED = {}



def broadcasted(f):
    """
    Proxying decorator. If the component is in server_mode, field the
    request. If not, send the request to the all servers in the
    component pool. Does not return a result.
    """

    def decorator(self, *args, **kwargs):
        if not self.initialized:
            LOGGER.debug("Delaying execution of %s "
                "until initialization." % self.server_methods[id(f)])
            reactor.callLater(5, INVERSE_SHARED[id(f)], self, *args, **kwargs)
            return
        if self.server_mode:
            maybeDeferred(f, self, *args, **kwargs)
        for remote_obj in self.remote_objs.values():
            try:
                d = remote_obj.callRemote(
                    self.server_methods[id(f)],
                    *args,
                    **kwargs)
            except pb.DeadReferenceError:
                self.server.disconnect_by_remote_obj(remote_obj)
            d.addErrback( self.trap_broadcasted_disconnect, remote_obj)
    SHARED[id(decorator)] = (f, id(f))
    INVERSE_SHARED[id(f)] = decorator
    return decorator


def check_response(data, obj, start):
    obj.wait_time += time.time() - start
    if isinstance(data, dict) and "_pb_failure" in data:
        return loads(data["_pb_failure"])
    return data


def shared(f):
    """
    Proxying decorator. If the component is in server_mode, field the
    request. If not, send the request to the component pool.
    """
    def decorator(self, *args, **kwargs):
        if not self.initialized:
            reactor.callLater(5, INVERSE_SHARED[id(f)], self, *args, **kwargs)
            return
        if self.server_mode:
            if "_remote_call" in kwargs:
                del kwargs["_remote_call"]
                d = maybeDeferred(f, self, *args, **kwargs)
                d.addErrback(lambda x:{"_pb_failure":dumps(x)})
                return d
            return maybeDeferred(f, self, *args, **kwargs)
        try:
            remote_obj = choice(self.remote_objs.values())
        except IndexError:
            d = Deferred()
            d.errback(ComponentException("No active %s "
                "connections." % self.__class__.__name__))
            return d
        kwargs["_remote_call"] = True
        try:
            d = remote_obj.callRemote(
                self.server_methods[id(f)],
                *args,
                **kwargs)
        except pb.DeadReferenceError:
            self.server.disconnect_by_remote_obj(remote_obj)
            return INVERSE_SHARED[id(f)](self, *args, **kwargs)
        d.addCallback(check_response, self, time.time())
        d.addErrback(
            self.trap_shared_disconnect,
            remote_obj,
            INVERSE_SHARED[id(f)],
            *args,
            **kwargs)
        return d
    SHARED[id(decorator)] = (f, id(f))
    INVERSE_SHARED[id(f)] = decorator
    return decorator


class Component(object):
    """
    Abstract class that proxies component requests.
    """

    initialized = False
    server_mode = False
    running = False
    requires = None
    wait_time = 0

    def __init__(self, server, server_mode):
        self.server = server
        self.server_mode = server_mode
        name = self.__class__.__name__.lower()
        self.remote_objs = self.server.component_servers[name]
        self.server_methods = {}
        check_method = lambda x: isinstance(x[1], types.MethodType)
        instance_methods = filter(check_method, inspect.getmembers(self))
        for instance_method in instance_methods:
            func = instance_method[1]
            func_id = id(func.__func__)
            if func_id in SHARED:
                name = "%s_%s" % (
                    func.im_class.__name__,
                    SHARED[func_id][0].__name__)
                setattr(server, "remote_%s" % name, func)
                self.server_methods[SHARED[func_id][1]] = name
        if not self.requires:
            self.requires = []
        # Shutdown before the reactor.
        reactor.addSystemEventTrigger(
            'before',
            'shutdown',
            self._shutdown)

    def trap_broadcasted_disconnect(self, failure, remote_obj):
        failure.trap(pb.PBConnectionLost)
        if self.running:
            LOGGER.error(failure)
        self.server.disconnect_by_remote_obj(remote_obj)
        return None

    def trap_shared_disconnect(self, failure, remote_obj, f, *args, **kwargs):
        failure.trap(pb.PBConnectionLost)
        if self.running:
            LOGGER.error(failure)
        self.server.disconnect_by_remote_obj(remote_obj)
        return f(self, *args, **kwargs)

    @inlineCallbacks
    def _initialize(self):
        if self.server_mode:
            yield maybeDeferred(self.initialize)
            self.initialized = True
        else:
            if self.__class__.__name__.lower() not in self.server.requires:
                self.initialized = True
        returnValue(None)

    @inlineCallbacks
    def _start(self):
        """Abstract initialization method."""
        self.running = True
        if self.server_mode:
            yield maybeDeferred(self.start)
        returnValue(None)

    def initialize(self):
        """Abstract initialization method."""
        pass

    def start(self):
        """Abstract start method."""
        pass

    @inlineCallbacks
    def _shutdown(self):
        self.running = False
        if self.server_mode:
            yield maybeDeferred(self.shutdown)
        returnValue(None)

    def shutdown(self):
        pass

