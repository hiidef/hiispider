#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""HiiSpider is a sophisticated asynchronous spider designed to work in a
distributed manner and handle thousands of requests per second.  It's capable
of writing results to both MySQL and Cassandra and features a """

VERSION = (0, 10, 3, 0)

#from servers import *
from plugin import HiiSpiderPlugin

__all__ = ['VERSION']

#__all__ = ['SchedulerServer', 'InterfaceServer', 'WorkerServer', 'TestingServer',
#    'CassandraServer', 'IdentityServer', 'HiiSpiderPlugin', 'VERSION']
