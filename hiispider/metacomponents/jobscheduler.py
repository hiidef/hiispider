#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Schedules jobs."""

import logging
from uuid import UUID
from .scheduler import Scheduler
from ..components import Logger, MySQL, JobQueue
from twisted.internet.defer import inlineCallbacks
from hiispider.components.base import shared


LOGGER = logging.getLogger(__name__)


def get_uuid_bytes(uuid):
    try:
        # Make sure the uuid is in bytes
        uuid_bytes = UUID(uuid).bytes
    except ValueError:
        LOGGER.error("Could not turn UUID into bytes: %s" % uuid)
    return uuid_bytes


class JobScheduler(Scheduler):

    removed_jobs = set([])
    requires = [Logger, MySQL, JobQueue]

    def __init__(self, server, config, server_mode, **kwargs):
        super(JobScheduler, self).__init__(
            server, 
            config, 
            server_mode, 
            server.jobqueue, 
            **kwargs)
        self.service_mapping = config["service_mapping"]
        self.server.expose(self.enqueue_uuid)
        self.server.expose(self.add_uuid)
        self.server.expose(self.remove_uuid)

    def is_valid(self, item):
        if item in self.removed_jobs:
            self.removed_jobs.remove(item)
            return False
        return True
    
    @shared
    def enqueue_uuid(self, uuid):
        self.queue.publish(get_uuid_bytes(uuid))

    @shared
    def add_uuid(self, uuid, job_type):
        return _add_uuid(self, uuid, job_type)

    def _add_uuid(self, uuid, job_type):
        if job_type in self.service_mapping:
            job_type = self.service_mapping[job_type]
        if job_type in self.server.functions:
            self.add(
                get_uuid_bytes(uuid), 
                int(self.server.functions[job_type]['interval']))

    @shared
    def remove_uuid(self, uuid):
        self.removed_jobs.add(get_uuid_bytes(uuid))

    @inlineCallbacks
    def start(self):
        yield super(JobScheduler, self).start()
        # Get jobs
        data = []
        start = 0
        while len(data) >= 100000 or start == 0:
            sql = """SELECT uuid, type
                     FROM spider_service
                     ORDER BY id LIMIT %s, 100000
                  """ % start
            start += 100000
            data = yield self.server.mysql.runQuery(sql)
            for row in data:
                self._add_uuid(row["uuid"], row["type"])
            LOGGER.debug("Added %s jobs to heap." % len(data))
        
