#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""HiiSpider is a sophisticated asynchronous spider designed to work in a
distributed manner and handle thousands of requests per second.  It's capable
of writing results to both MySQL and Cassandra and features a """

__all__ = ['SchedulerServer', 'InterfaceServer', 'WorkerServer',
           'HiiSpiderPlugin', 'VERSION']

VERSION = (0, 10, 1, 3)

from .servers import SchedulerServer
from .servers import InterfaceServer
from .servers import WorkerServer
from .plugin import HiiSpiderPlugin
