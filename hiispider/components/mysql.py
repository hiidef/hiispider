#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Communicates with MySQL
"""

import logging
from copy import copy
from MySQLdb.cursors import DictCursor
from twisted.enterprise import adbapi
from .logger import Logger
from .base import Component, shared


LOGGER = logging.getLogger(__name__)


class MySQL(Component):

    """Implements MySQL's runquery method as an RPC."""

    mysql = None

    def __init__(self, server, config, server_mode, **kwargs):
        super(MySQL, self).__init__(server, server_mode)
        config = copy(config)
        config.update(kwargs)
        self.db = config["mysql_database"]
        self.port = config.get("mysql_port", 3306)
        self.user = config["mysql_username"]
        self.passwd = config["mysql_password"]
        self.host = config["mysql_host"]

    def initialize(self):
        LOGGER.info('Initializing %s' % self.__class__.__name__) 
        self.mysql = adbapi.ConnectionPool(
            "MySQLdb",
            db=self.db,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            host=self.host,
            cp_reconnect=True,
            cursorclass=DictCursor)
        LOGGER.info('%s initialized.' % self.__class__.__name__)

    @shared
    def runQuery(self, *args, **kwargs):
        return self.mysql.runQuery(*args, **kwargs)

    @shared
    def runOperation(self, *args, **kwargs):
        return self.mysql.runOperation(*args, **kwargs)
