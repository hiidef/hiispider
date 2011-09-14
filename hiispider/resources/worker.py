#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Resources for the workerserver."""

import time
import simplejson
from hiispider.resources.base import BaseResource

class WorkerResource(BaseResource):
    isLeaf = True

    def __init__(self, workerserver):
        self.workerserver = workerserver
        BaseResource.__init__(self)

    def render(self, request):
        request.setHeader('Content-type', 'text/javascript; charset=UTF-8')
        data = {'completed': self.workerserver.jobs_complete,
                'queued': len(self.workerserver.jobs_semaphore.waiting),
                'active': len(self.workerserver.active_jobs),
                'age': (time.time() - self.workerserver.t0),
               }
        return simplejson.dumps(data)

