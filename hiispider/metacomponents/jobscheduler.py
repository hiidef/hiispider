#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Schedules jobs."""

import logging
from uuid import UUID
from hiispider.metacomponents.scheduler import Scheduler
from hiispider.components import Logger, MySQL, JobQueue
from twisted.internet.defer import inlineCallbacks
from hiispider.components.base import shared


LOGGER = logging.getLogger(__name__)


def get_uuid_bytes(uuid):
    try:
        # Make sure the uuid is in bytes
        uuid_bytes = UUID(uuid).bytes
        return uuid_bytes
    except ValueError:
        LOGGER.error("Could not turn UUID into bytes: %s" % uuid)
        return None


class JobScheduler(Scheduler):

    removed_jobs = set([])
    requires = [Logger, MySQL, JobQueue]

    def __init__(self, server, config, server_mode, **kwargs):
        super(JobScheduler, self).__init__(server, config, server_mode,
            server.jobqueue, **kwargs)
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
    def add_uuid(self, uuid, type):
        return self._add_uuid(uuid, type)

    def _add_uuid(self, uuid, type):
        if type in self.service_mapping:
            type = self.service_mapping[type]
        if type in self.server.functions:
            interval = self.server.functions[type]['interval']
            bytes = get_uuid_bytes(uuid)
            if bytes:
                self.add(bytes, int(interval))

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
            if len(data):
                LOGGER.debug("Added %s jobs to heap." % len(data))

