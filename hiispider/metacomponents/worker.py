#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Executes jobs, reports results to a number of components."""

import time
from pprint import pformat
from copy import copy
import logging
from collections import defaultdict
from traceback import format_exc
from random import random
from twisted.internet import task, reactor
from twisted.internet.defer import inlineCallbacks
from .jobexecuter import JobExecuter


LOGGER = logging.getLogger(__name__)  


class Worker(JobExecuter):
    
    """Initializes workers that communicate with external components."""

    delta_enabled = False
    active_workers = 0
    simultaneous_jobs = 30
    jobs = set([])
    job_speed_start = time.time()
    job_speed_report_loop = None
    timer = defaultdict(lambda:0)
    timer_count = defaultdict(lambda:0)
    timer_starts = {}
    job_queue = []

    def __init__(self, server, config, server_mode, **kwargs):
        super(Worker, self).__init__(server, config, server_mode, **kwargs)
        config = copy(config)
        config.update(kwargs)
        self.delta_enabled = config.get('delta_enabled', False)
        self.delta_sample_rate = config.get('delta_sample_rate', 1.0)

    def start(self):
        super(Worker, self).start()
        self.job_speed_report_loop = task.LoopingCall(self._job_speed_report)
        self.job_speed_report_loop.start(60, False)
        for i in range(0, self.simultaneous_jobs):
            self.work()
        self._job_speed_report()

    def _job_speed_report(self):
        jps = self.jobs_complete / (time.time() - self.job_speed_start)
        fps = self.job_failures / (time.time() - self.job_speed_start)
        LOGGER.info("Total times:\n %s" % pformat(sorted(self.timer.items(), key=lambda x:x[1])))
        LOGGER.info("Average times:\n %s" % pformat(sorted([(x, float(self.timer[x])/self.timer_count[x]) for x in self.timer], key=lambda x:x[1])))
        LOGGER.info("Active jobs:\n %s" % self.jobs)
        LOGGER.info("%s jobs per second, %s failures per second." % (jps, fps))
        LOGGER.info("Wait time by component:\n%s" % "\n".join(["%s:%s" % (x.__class__.__name__, x.wait_time) for x in self.server.components]))
        self.job_speed_start = time.time()
        self.jobs_complete = 0
        self.job_failures = 0

    def shutdown(self):
        if self.job_speed_report_loop:
            self.job_speed_report_loop.stop()

    def time_start(self, task_id):
        self.timer_starts[task_id] = time.time()

    def time_end(self, task_id, task_name):
        self.timer_count[task_name] += 1
        self.timer[task_name] += time.time() - self.timer_starts[task_id]
        del self.timer_starts[task_id]

    @inlineCallbacks
    def work(self):
        if not self.running:
            return
        r = random()
        self.time_start(r)
        if len(self.job_queue) < 10:
            try:
                jobs = yield self.server.jobgetter.getJobs()
                self.job_queue.extend(jobs)
            except Exception:
                self.time_end(r, "job_wait")
                LOGGER.error(format_exc())
                reactor.callLater(0, self.work)
                return
        job = self.job_queue.pop()
        self.time_end(r, "job_wait")
        self.jobs.add(job)
        try:
            self.time_start(job.uuid)
            data = yield self.executeJob(job)
            self.time_end(job.uuid, job.function_name)
        except Exception:
            self.time_end(job.uuid, job.function_name)
            self.jobs.remove(job)
            LOGGER.error("Job: %s\n%s" % (job.function_name, format_exc()))
            reactor.callLater(0, self.work)
            return
        if data is None:
            self.jobs.remove(job)
            reactor.callLater(0, self.work)
            return
        if self.deltas:
            try:
                self.time_start(job.uuid)
                yield self.generate_deltas(data, job)
                self.time_end(job.uuid, "%s.delta" % job.function_name)
            except Exception:
                self.time_end(job.uuid, "%s.delta" % job.function_name)
                LOGGER.error(format_exc())
        try:
            self.time_start(job.uuid)
            self.server.cassandra.setData(data, job)
            self.time_end(job.uuid, "%s.cassandra" % job.function_name)
        except Exception:
            self.time_end(job.uuid, "%s.cassandra" % job.function_name)
            LOGGER.error("Job: %s\n%s" % (job.function_name, format_exc()))
        self.jobs.remove(job)
        reactor.callLater(0, self.work)


    def _deltas(self):
        if self.delta_enabled:
            return random.random() <= self.delta_sample_rate
        return False

    deltas = property(_deltas)


