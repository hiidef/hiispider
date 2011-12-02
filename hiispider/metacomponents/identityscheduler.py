#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Scheules Identity checks."""

import logging
from hiispider.metacomponents.scheduler import Scheduler
from hiispider.components import Logger, MySQL, IdentityQueue
from twisted.internet.defer import inlineCallbacks
from hiispider.components.base import shared


LOGGER = logging.getLogger(__name__)


class IdentityScheduler(Scheduler):
    
    redistribute = False
    removed_user_ids = set([])
    requires = [Logger, MySQL, IdentityQueue]

    def __init__(self, server, config, server_mode, **kwargs):
        super(IdentityScheduler, self).__init__(server, config, server_mode,
            server.identityqueue, **kwargs)
        self.server.expose(self.enqueue_user_id)
        self.server.expose(self.add_user_id)
        self.server.expose(self.remove_user_id)

    def is_valid(self, user_id):
        if user_id in self.removed_user_ids:
            self.removed_user_ids.remove(str(user_id))
            return False
        return True

    @shared
    def enqueue_user_id(self, user_id):
        self.queue.publish(str(user_id))

    @shared
    def add_user_id(self, user_id):
        return self._add_user_id(user_id)

    def _add_user_id(self, user_id):
        self.add(str(user_id), 60*60*2)

    @shared
    def remove_user_id(self, user_id):
        self.removed_user_ids.add(str(user_id))

    @inlineCallbacks
    def start(self):
        super(IdentityScheduler, self).start()
        data = []
        start = 0
        while len(data) >= 100000 or start == 0:
            sql = """SELECT id
                     FROM auth_user
                     ORDER BY id LIMIT %s, 100000
                  """ % start
            start += 100000
            data = yield self.server.mysql.runQuery(sql)
            for row in data:
                self._add_user_id(row["id"])
            if len(data):
                LOGGER.debug("Added %s user_ids to heap." % len(data))

