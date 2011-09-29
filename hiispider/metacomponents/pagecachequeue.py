from ..components import Queue


class PagecacheQueue(Queue):

    def __init__(self, server, config, address=None, **kwargs):
        kwargs["amqp_vhost"] = config["amqp_pagecache_vhost"]
        super(PagecacheQueue, self).__init__(server, config, address=None, **kwargs)