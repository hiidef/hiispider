#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Communicates with the Identity Queue.
"""


from .queue import Queue


class IdentityQueue(Queue):
     """Connects to the identity vhost."""


    def __init__(self, server, config, server_mode, **kwargs):
        kwargs["amqp_vhost"] = config["amqp_identity_vhost"]
        super(IdentityQueue, self).__init__(
            server, 
            config, 
            server_mode, 
            **kwargs)