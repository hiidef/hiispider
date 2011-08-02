from uuid import UUID
import simplejson
import logging
from hashlib import sha256
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import task
from txamqp.content import Content
from ...amqp import amqp as AMQP

logger = logging.getLogger(__name__)

class AMQPMixin(object):

    amqp_queue_size = 0
    amqp_pagecache_queue_size = 0
    # Define these in case we have an early shutdown.
    chan = None
    pagecache_chan = None

    def setupAMQP(self, config):
        # Create AMQP Connection
        # AMQP connection parameters
        self.amqp_host = config["amqp_host"]
        self.amqp_vhost = config["amqp_vhost"]
        self.amqp_port = config.get("amqp_port", 5672)
        self.amqp_username = config["amqp_username"]
        self.amqp_password = config["amqp_password"]
        self.amqp_queue = config["amqp_queue"]
        self.amqp_exchange = config["amqp_exchange"]
        self.amqp_prefetch_count = config.get("amqp_prefetch_count", 50)
        # Pagecache
        self.amqp_pagecache_vhost = config["amqp_pagecache_vhost"]
        self.pagecache_web_server_host = config["pagecache_web_server_host"]

    @inlineCallbacks
    def startJobQueue(self):
        self.conn = yield AMQP.createClient(
            self.amqp_host,
            self.amqp_vhost,
            self.amqp_port)
        self.auth = yield self.conn.authenticate(
            self.amqp_username,
            self.amqp_password)
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
        self.jobstatusloop = task.LoopingCall(self.jobQueueStatusCheck)
        self.jobstatusloop.start(60)

    @inlineCallbacks
    def startPageCacheQueue(self):
        # Setup pagecache queue
        self.pagecache_conn = yield AMQP.createClient(
            self.amqp_host,
            self.amqp_pagecache_vhost,
            self.amqp_port)
        yield self.pagecache_conn.authenticate(self.amqp_username, self.amqp_password)
        self.pagecache_chan = yield self.pagecache_conn.channel(1)
        yield self.pagecache_chan.channel_open()
        # Create pagecache Queue
        yield self.pagecache_chan.queue_declare(
            queue=self.amqp_queue,
            durable=False,
            exclusive=False,
            auto_delete=False)
        # Create pagecache Exchange
        yield self.pagecache_chan.exchange_declare(
            exchange=self.amqp_exchange,
            type="fanout",
            durable=False,
            auto_delete=False)
        yield self.pagecache_chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
        self.pagecachestatusloop = task.LoopingCall(self.pagecacheQueueStatusCheck)
        self.pagecachestatusloop.start(60)

    @inlineCallbacks
    def stopJobQueue(self):
        self.jobstatusloop.stop()
        logger.info('Closing job queue')
        yield self.chan.channel_close()
        chan0 = yield self.conn.channel(0)
        yield chan0.connection_close()

    @inlineCallbacks
    def stopPageCacheQueue(self):
        self.pagecachestatusloop.stop()
        try:
            logger.info('Closing pagecache queue')
            yield self.pagecache_chan.channel_close()
            pagecache_chan0 = yield self.pagecache_conn.channel(0)
            yield pagecache_chan0.connection_close()
        except Exception, e:
            logger.error("Could not close pagecache queue: %s" % e)


    @inlineCallbacks
    def jobQueueStatusCheck(self):
        yield self.chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
        queue_status = yield self.chan.queue_declare(
            queue=self.amqp_queue,
            passive=True)
        self.amqp_queue_size = queue_status.fields[1]
        logger.debug('Job queue size: %d' % self.amqp_queue_size)

    @inlineCallbacks
    def pagecacheQueueStatusCheck(self):
        yield self.pagecache_chan.queue_bind(
            queue=self.amqp_queue,
            exchange=self.amqp_exchange)
        pagecache_queue_status = yield self.pagecache_chan.queue_declare(
            queue=self.amqp_queue,
            passive=True)
        self.amqp_pagecache_queue_size = pagecache_queue_status.fields[1]
        logger.debug('Pagecache queue size: %d' % self.amqp_pagecache_queue_size)

    @inlineCallbacks
    def getJobUUID(self):
        msg = yield self.queue.get()
        if msg.delivery_tag:
            try:
                logger.debug('basic_ack for delivery_tag: %s' % msg.delivery_tag)
                yield self.chan.basic_ack(msg.delivery_tag)
            except Exception, e:
                logger.error('basic_ack Error: %s' % e)
        returnValue(UUID(bytes=msg.content.body).hex)

    @inlineCallbacks
    def clearPageCache(self, job):
        # Add to the pagecache queue to signal Django to
        # regenerate / clear the cache.
        if not self.amqp_pagecache_vhost:
            return
        if self.amqp_pagecache_queue_size > 100000:
            # FIXME: What does this event mean?  Should we count it?
            logger.debug('Pagecache Queue Size has exceeded 100,000 items')
            return
        pagecache_msg = {}
        if "host" in job.user_account and job.user_account['host']:
            cache_key = job.user_account['host']
            pagecache_msg['host'] = job.user_account['host']
        else:
            cache_key = '%s/%s' % (
                self.pagecache_web_server_host,
                job.user_account['username'])
        pagecache_msg['username'] = job.user_account['username']
        pagecache_msg['cache_key'] = sha256(cache_key).hexdigest()
        msg = Content(simplejson.dumps(pagecache_msg))
        try:
            yield self.pagecache_chan.basic_publish(
                exchange=self.amqp_exchange,
                content=msg)
        except Exception, e:
            logger.error('Pagecache Error: %s' % str(error))
