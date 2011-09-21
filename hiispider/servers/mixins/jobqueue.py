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


class JobQueueMixin(AMQPMixin):

    amqp_jobs_queue_size = 0
    jobs_chan = None

    def setupJobQueue(self, config):
        self.setupAMQP(config)
        self.amqp_jobs_vhost = config["amqp_jobs_vhost"]

    @inlineCallbacks
    def startJobQueue(self):
        logger.debug("Starting Job Queue.")
        self.jobs_conn = yield AMQP.createClient(
            self.amqp_host,
            self.amqp_jobs_vhost,
            self.amqp_port)
        yield self.jobs_conn.authenticate(self.amqp_username, self.amqp_password)
        self.jobs_chan = yield self.jobs_conn.channel(2)
        yield self.jobs_chan.channel_open()
        yield self.jobs_chan.basic_qos(prefetch_count=self.amqp_prefetch_count)
        # Create Queue
        yield self.jobs_chan.queue_declare(
            queue=self.amqp_queue,
            durable=False,
            exclusive=False,
            auto_delete=False)
        # Create Exchange
        yield self.jobs_chan.exchange_declare(
            exchange=self.amqp_exchange,
            type="fanout",
            durable=False,
            auto_delete=False)
        yield self.jobs_chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
        yield self.jobs_chan.basic_consume(queue=self.amqp_queue,
            no_ack=False,
            consumer_tag="hiispider_consumer")
        self.jobs_rabbit_queue = yield self.jobs_conn.queue("hiispider_consumer")
        self.jobstatusloop = task.LoopingCall(self.jobQueueStatusCheck)
        self.jobstatusloop.start(60)

    @inlineCallbacks
    def stopJobQueue(self):
        self.jobstatusloop.stop()
        logger.info('Closing job queue')
        yield self.jobs_chan.channel_close()
        chan0 = yield self.jobs_conn.channel(0)
        yield chan0.connection_close()
    
    @inlineCallbacks
    def jobQueueStatusCheck(self):
        yield self.jobs_chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
        queue_status = yield self.jobs_chan.queue_declare(
            queue=self.amqp_queue,
            passive=True)
        self.amqp_jobs_queue_size = queue_status.fields[1]
        logger.debug('Job queue size: %d' % self.amqp_jobs_queue_size)
    
    @inlineCallbacks
    def getJobUUID(self):
        msg = yield self.jobs_rabbit_queue.get()
        if msg.delivery_tag:
            try:
                logger.debug('basic_ack for delivery_tag: %s' % msg.delivery_tag)
                yield self.jobs_chan.basic_ack(msg.delivery_tag)
            except Exception, e:
                logger.error('basic_ack Error: %s' % e)
        returnValue(UUID(bytes=msg.content.body).hex)

