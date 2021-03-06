#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Communicates with RabbitMQ.
"""

import sys
from copy import copy
import logging
from traceback import format_exc
from twisted.internet import task
from txamqp.client import Closed
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient
from txamqp.content import Content
import txamqp.spec
from twisted.spread import pb
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue

from hiispider.components.base import Component, shared
from hiispider.components.logger import Logger
from specs import v0_8


LOGGER = logging.getLogger(__name__)


class Queue(Component):

    """Implements the shared 'get' method which returns a message body."""

    conn = None
    chan = None
    queue_size = 0
    statusloop = None
    queue = None

    def __init__(self, server, config, server_mode, **kwargs):
        super(Queue, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        self.amqp = {
            "host":config["amqp_host"],
            "port":config.get("amqp_port", 5672),
            "username":config["amqp_username"],
            "password":config["amqp_password"],
            "queue":config["amqp_queue"],
            "exchange":config["amqp_exchange"],
            "prefetch_count":config["amqp_prefetch_count"],
            "vhost":config["amqp_vhost"]}

    @inlineCallbacks
    def initialize(self):
        LOGGER.info("Initializing %s" % self.__class__.__name__)
        client = ClientCreator(reactor,
            AMQClient,
            delegate=TwistedDelegate(),
            vhost=self.amqp["vhost"],
            spec=txamqp.spec.loadString(v0_8),
            heartbeat=0)
        self.conn = yield client.connectTCP(
            self.amqp["host"],
            self.amqp["port"],
            timeout=sys.maxint)
        yield self.conn.authenticate(
            self.amqp["username"],
            self.amqp["password"])
        self.chan = yield self.conn.channel(2)
        yield self.chan.channel_open()
        yield self.chan.basic_qos(prefetch_count=self.amqp["prefetch_count"])
        # Create Queue
        yield self.chan.queue_declare(
            queue=self.amqp["queue"],
            durable=False,
            exclusive=False,
            auto_delete=False)
        # Create Exchange
        yield self.chan.exchange_declare(
            exchange=self.amqp["exchange"],
            type="fanout",
            durable=False,
            auto_delete=False)
        yield self.chan.queue_bind(
            queue=self.amqp["queue"],
            exchange=self.amqp["exchange"])
        yield self.chan.basic_consume(queue=self.amqp["queue"],
            no_ack=False,
            consumer_tag="hiispider_consumer")
        self.queue = yield self.conn.queue("hiispider_consumer")
        self.statusloop = task.LoopingCall(self.status_check)
        self.statusloop.start(60)
        LOGGER.info('%s initialized.' % self.__class__.__name__)

    @shared
    def publish(self, item):
        self.chan.basic_publish(
            exchange=self.amqp["exchange"],
            content=Content(item))

    @inlineCallbacks
    def shutdown(self):
        if self.statusloop:
            self.statusloop.stop()
        LOGGER.info('Closing %s' % self.__class__.__name__)
        yield self.queue.close()
        yield self.chan.channel_close()
        chan0 = yield self.conn.channel(0)
        yield chan0.connection_close()
        LOGGER.info('%s closed.' % self.__class__.__name__)

    @shared
    @inlineCallbacks
    def get(self, *args, **kwargs):
        msg = yield self.queue.get(*args, **kwargs)
        self.chan.basic_ack(msg.delivery_tag)
        returnValue(msg.content.body)

    @inlineCallbacks
    def status_check(self):
        try:
            queue_status = yield self.chan.queue_declare(
                queue=self.amqp["queue"],
                passive=True)
            self.queue_size = queue_status.fields[1]
            LOGGER.debug("%s queue size: "
                "%d" % (self.__class__.__name__, self.queue_size))
        except:
            LOGGER.error(format_exc())
            self.reconnect()

    @inlineCallbacks
    def reconnect(self):
        try:
            yield self.shutdown()
        except Exception, e:
            LOGGER.error(format_exc())
        try:
            yield self.initialize()
        except Exception, e:
            LOGGER.error(format_exc())

