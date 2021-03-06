#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Communicates with Page Cache Queue
"""

import logging
import ujson as json
from hashlib import sha256
from txamqp.content import Content
from twisted.internet.defer import inlineCallbacks
from .base import shared
from .queue import Queue


LOGGER = logging.getLogger(__name__)


class PageCacheQueue(Queue):
    """
    Implements 'clear' - an RPC method that inserts a cache key into
    the queue.
    """

    def __init__(self, server, config, server_mode, **kwargs):
        kwargs["amqp_vhost"] = config["amqp_pagecache_vhost"]
        super(PageCacheQueue, self).__init__(server, config, server_mode, **kwargs)
        self.pagecache_web_server_host = config["pagecache_web_server_host"]

    @shared
    @inlineCallbacks
    def clear(self, job):
        # Add to the pagecache queue to signal Django to
        # regenerate / clear the cache.
        if self.queue_size > 100000:
            LOGGER.error("%s size has exceeded 100,000 "
                "items" % self.__class__.__name__)
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
        msg = Content(json.dumps(pagecache_msg))
        try:
            yield self.chan.basic_publish(
                exchange=self.amqp["exchange"],
                content=msg)
        except Exception, e:
            LOGGER.error('Pagecache Error: %s' % str(e))
