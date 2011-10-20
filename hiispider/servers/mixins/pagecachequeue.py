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


class PageCacheQueueMixin(AMQPMixin):
    
    amqp_pagecache_queue_size = 0
    pagecache_chan = None

    def setupPageCacheQueue(self, config):
        self.setupAMQP(config)
        self.amqp_pagecache_vhost = config["amqp_pagecache_vhost"]
        self.pagecache_web_server_host = config["pagecache_web_server_host"]

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
    def clearPageCache(self, job):
        # Add to the pagecache queue to signal Django to
        # regenerate / clear the cache.
        if not self.amqp_pagecache_vhost:
            return
        if self.amqp_pagecache_queue_size > 100000:
            logger.error('Pagecache Queue Size has exceeded 100,000 items')
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
            logger.error('Pagecache Error: %s' % str(e))
