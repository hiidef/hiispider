#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Communicates with the Job Queue.
"""

from hiispider.components.queue import Queue


class JobQueue(Queue):
    """Connects to the job vhost."""

    def __init__(self, server, config, server_mode, **kwargs):
        kwargs["amqp_vhost"] = config["amqp_jobs_vhost"]
        super(JobQueue, self).__init__(server, config, server_mode, **kwargs)

