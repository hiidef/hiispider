#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Schedules jobs."""

from .scheduler import Scheduler
import logging
from ..components import Logger, MySQL, IdentityQueue

LOGGER = logging.getLogger(__name__)


class IdentityScheduler(Scheduler):

    requires = [Logger, MySQL, IdentityQueue]

    def __init__(self, server, config, server_mode, **kwargs):
        super(IdentityScheduler, self).__init__(
            server, 
            config, 
            server_mode, 
            server.identityqueue,
            **kwargs)