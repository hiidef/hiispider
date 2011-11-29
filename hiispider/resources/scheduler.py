#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Scheduler Resource"""

from hiispider.resources.base import BaseResource

class SchedulerResource(BaseResource):

    isLeaf = True

    def __init__(self, schedulerserver):
        self.schedulerserver = schedulerserver
        BaseResource.__init__(self)

    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        return "{}"
