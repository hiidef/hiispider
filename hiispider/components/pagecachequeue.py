from .base import shared
from .queue import Queue
import logging
import simplejson
from txamqp.content import Content
from hashlib import sha256
from twisted.internet.defer import inlineCallbacks

LOGGER = logging.getLogger(__name__)

class PagecacheQueue(Queue):

    def __init__(self, server, config, address=None, allow_clients=None, **kwargs):
        kwargs["amqp_vhost"] = config["amqp_pagecache_vhost"]
        super(PagecacheQueue, self).__init__(server, config, address=address, allow_clients=allow_clients, **kwargs)
        self.pagecache_web_server_host = config["pagecache_web_server_host"]

    @shared
    @inlineCallbacks
    def clear(self, job):
        # Add to the pagecache queue to signal Django to
        # regenerate / clear the cache.
        if self.queue_size > 100000:
            LOGGER.error('%s size has exceeded 100,000 items' % self.__class__.__name__)
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
            yield self.chan.basic_publish(
                exchange=self.amqp_exchange,
                content=msg)
        except Exception, e:
            LOGGER.error('Pagecache Error: %s' % str(e))