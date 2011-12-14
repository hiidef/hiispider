#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twisted.trial import unittest
from twisted.web import server, resource
from twisted.internet import reactor
from hiispider.pagegetter2 import PageGetter
from twisted.internet.defer import inlineCallbacks
from hiispider.server import Server
from hiispider.components import Redis, Logger
from config import CONFIG

class Webserver(resource.Resource):
    
    isLeaf = True

    def render_GET(self, request):
        return getattr(self, request.path.strip("/"))(request)

    def get(self, request):
        return "Hello world."


class PageGetter2TestCase(unittest.TestCase):

    @inlineCallbacks
    def setUp(self):
        self.port = reactor.listenTCP(8080, server.Site(Webserver()))
        self.hs = Server(CONFIG, "127.0.0.1", components=[Redis, Logger])
        self.pg = PageGetter(self.hs.redis, rq=self.hs.rq)
        yield self.hs.start()

    @inlineCallbacks
    def tearDown(self):
        self.port.stopListening()
        yield self.hs.shutdown()

    @inlineCallbacks
    def test_webserver(self):
        data = yield self.pg.getPage("http://127.0.0.1:8080/get")
        print data