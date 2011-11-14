#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Executes new jobs on behalf of the Django application."""

from uuid import uuid4
from twisted.internet.defer import inlineCallbacks, returnValue
from ..components import *
from ..metacomponents import *
import logging


LOGGER = logging.getLogger(__name__)


class Testing(JobExecuter):

    allow_clients = False

    def __init__(self, server, config, server_mode, **kwargs):
        super(Testing, self).__init__(server, server_mode)
        
