from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue
from copy import copy
from twisted.internet import reactor
from ..amqp import amqp as AMQP
import logging
from twisted.internet import task
from txamqp.client import Closed
from traceback import format_exc
from twisted.spread import pb
from logger import Logger

LOGGER = logging.getLogger(__name__)


class Queue(Component):

    conn = None
    chan = None
    queue_size = 0
    statusloop = None

    def __init__(self, server, config, server_mode, **kwargs):
        super(Queue, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        self.amqp_host = config["amqp_host"]
        self.amqp_port = config.get("amqp_port", 5672)
        self.amqp_username = config["amqp_username"]
        self.amqp_password = config["amqp_password"]
        self.amqp_queue = config["amqp_queue"]
        self.amqp_exchange = config["amqp_exchange"]
        self.amqp_prefetch_count = config["amqp_prefetch_count"]
        self.amqp_vhost = config["amqp_vhost"]
        
    @inlineCallbacks
    def initialize(self):
        LOGGER.info("Initializing %s" % self.__class__.__name__)    
        self.conn = yield AMQP.createClient(
            self.amqp_host,
            self.amqp_vhost,
            self.amqp_port)
        yield self.conn.authenticate(self.amqp_username, self.amqp_password)
        self.chan = yield self.conn.channel(2)
        yield self.chan.channel_open()
        yield self.chan.basic_qos(prefetch_count=self.amqp_prefetch_count)
        # Create Queue
        yield self.chan.queue_declare(
            queue=self.amqp_queue,
            durable=False,
            exclusive=False,
            auto_delete=False)
        # Create Exchange
        yield self.chan.exchange_declare(
            exchange=self.amqp_exchange,
            type="fanout",
            durable=False,
            auto_delete=False)
        yield self.chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
        yield self.chan.basic_consume(queue=self.amqp_queue,
            no_ack=False,
            consumer_tag="hiispider_consumer")
        self.queue = yield self.conn.queue("hiispider_consumer")
        self.statusloop = task.LoopingCall(self.status_check)
        self.statusloop.start(60)
        LOGGER.info('%s initialized.' % self.__class__.__name__)

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
    def get(self, *args, **kwargs):
        d = self.queue.get(*args, **kwargs)
        d.addCallback(self.basic_ack)
        return d

    def basic_ack(self, msg):
        self.chan.basic_ack(msg.delivery_tag)
        return msg.content.body
    
    @inlineCallbacks
    def status_check(self):
        try:
            queue_status = yield self.chan.queue_declare(
                queue=self.amqp_queue,
                passive=True)
            self.queue_size = queue_status.fields[1]
            LOGGER.debug('%s queue size: %d' % (self.__class__.__name__, self.queue_size))
        except:
            LOGGER.error(format_exc())
            self.reconnect()
    
    @inlineCallbacks
    def reconnect(self):
        try:
            yield self.shutdown()
        except:
            LOGGER.error(format_exc())
        try:
            yield self.initialize()
        except:
            LOGGER.error(format_exc())

