from .base import Component, shared
from twisted.internet.defer import inlineCallbacks, Deferred
from copy import copy
from twisted.internet import reactor
from ..amqp import amqp as AMQP


class Queue(Component):

    conn = None
    chan = None

    def __init__(self, config, **kwargs):
        config = copy(config)
        config.update(kwargs)
        self.amqp_host = config["amqp_host"]
        self.amqp_port = config.get("amqp_port", 5672)
        self.amqp_username = config["amqp_username"]
        self.amqp_password = config["amqp_password"]
        self.amqp_queue = config["amqp_queue"]
        self.amqp_exchange = config["amqp_exchange"]
        self.amqp_prefetch_count = config["amqp_prefetch_count"]
        self.amqp_jobs_vhost = config["amqp_jobs_vhost"]
        self.amqp_setup = True
    
    def _start(self, start_deferred):
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
            consumer_tag="hiispider")
        start_deferred.callback("Queue started successfully.")
    
    @shared
    def hello(self):
        return "World"
