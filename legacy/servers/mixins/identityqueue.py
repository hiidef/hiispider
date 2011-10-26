from uuid import UUID
import simplejson
import logging
from hashlib import sha256
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import task
from txamqp.content import Content
from ...amqp import amqp as AMQP
from .amqp import AMQPMixin


logger = logging.getLogger(__name__)


class IdentityQueueMixin(AMQPMixin):

    amqp_identity_queue_size = 0
    identity_chan = None

    def setupIdentityQueue(self, config):
        self.setupAMQP(config)
        self.amqp_identity_vhost = config["amqp_identity_vhost"]

    @inlineCallbacks
    def startIdentityQueue(self):
        self.identity_conn = yield AMQP.createClient(
            self.amqp_host,
            self.amqp_identity_vhost,
            self.amqp_port)
        yield self.identity_conn.authenticate(self.amqp_username, self.amqp_password)
        self.identity_chan = yield self.identity_conn.channel(2)
        yield self.identity_chan.channel_open()
        yield self.identity_chan.basic_qos(prefetch_count=self.amqp_prefetch_count)
        # Create Queue
        yield self.identity_chan.queue_declare(
            queue=self.amqp_queue,
            durable=False,
            exclusive=False,
            auto_delete=False)
        # Create Exchange
        yield self.identity_chan.exchange_declare(
            exchange=self.amqp_exchange,
            type="fanout",
            durable=False,
            auto_delete=False)
        yield self.identity_chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
        yield self.identity_chan.basic_consume(queue=self.amqp_queue,
            no_ack=False,
            consumer_tag="hiispider_consumer")
        self.identity_rabbit_queue = yield self.identity_conn.queue("hiispider_consumer")
        self.identitytatusloop = task.LoopingCall(self.identityQueueStatusCheck)
        self.identitytatusloop.start(60)

    @inlineCallbacks
    def stopIdentityQueue(self):
        self.identitytatusloop.stop()
        logger.info('Closing identity queue')
        yield self.identity_chan.channel_close()
        chan0 = yield self.identity_conn.channel(0)
        yield chan0.connection_close()
    
    @inlineCallbacks
    def identityQueueStatusCheck(self):
        yield self.identity_chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
        queue_status = yield self.identity_chan.queue_declare(
            queue=self.amqp_queue,
            passive=True)
        self.amqp_identity_queue_size = queue_status.fields[1]
        logger.debug('Identity queue size: %d' % self.amqp_identity_queue_size)
    
    @inlineCallbacks
    def getIdentityUserID(self):
        msg = yield self.identity_rabbit_queue.get()
        if msg.delivery_tag:
            try:
                logger.debug('basic_ack for delivery_tag: %s' % msg.delivery_tag)
                yield self.identity_chan.basic_ack(msg.delivery_tag)
            except Exception, e:
                logger.error('basic_ack Error: %s' % e)
        returnValue(int(msg.content.body))
