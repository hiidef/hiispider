#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Exposed resource."""

import sys
from twisted.web import server
from twisted.internet.defer import maybeDeferred
from hiispider.resources.base import BaseResource

class ExposedResource(BaseResource):

    isLeaf = True

    def __init__(self, server, function_name):
        self.primary_server = server
        self.function_name = function_name
        BaseResource.__init__(self)

    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        kwargs = {}
        for key in request.args:
            kwargs[key.replace('.', '_')] = request.args[key][0]
        d = maybeDeferred(self.primary_server.executeExposedFunction, self.function_name, **kwargs)
        d.addCallback(self._successResponse)
        d.addErrback(self._errorResponse)
        d.addCallback(self._immediateResponse, request)
        return server.NOT_DONE_YET
