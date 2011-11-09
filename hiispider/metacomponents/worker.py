#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Executes jobs, reports results to a number of components."""

import time
import logging
from pprint import pformat
from copy import copy
from collections import defaultdict
from traceback import format_exc
from random import random

from twisted.internet import task, reactor
from twisted.internet.defer import inlineCallbacks

from hiispider.metacomponents.jobexecuter import JobExecuter
from hiispider.metacomponents.jobgetter import JobGetter
from hiispider.metacomponents.pagegetter import PageGetter
from hiispider.components import *


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
    getting_jobs = False
    requires = [Logger, Stats, MySQL, JobHistoryRedis, JobGetter, PageGetter, PageCacheQueue, Cassandra]

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
        total_time = sum(self.timer.values())
        total_count = sum(self.timer_count.values())
        mean_time = total_time / len(self.timer)
        mean_time_per_call = sum([self.timer[x]/self.timer_count[x] for x in self.timer]) / len(self.timer)
        total_times = [(x[0], x[1] / mean_time) for x in self.timer.items()]
        average_times = [(x, self.timer[x]/(self.timer_count[x] * mean_time_per_call)) for x in self.timer]
        LOGGER.info("Total times:\n %s" % pformat(sorted(total_times, key=lambda x:x[1])))
        LOGGER.info("Average times:\n %s" % pformat(sorted(average_times, key=lambda x:x[1])))
        LOGGER.info("Active jobs:\n %s" % pformat(sorted([(x.function_name, round(time.time() - x.start, 2), x.uuid) for x in self.jobs], key=lambda x:x[1])))
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

    def time_end(self, task_id, task_name, add=0):
        self.timer_count[task_name] += 1
        self.timer[task_name] += add + time.time() - self.timer_starts[task_id]
        del self.timer_starts[task_id]

    @inlineCallbacks
    def getJobs(self):
        self.getting_jobs = True
        try:
            jobs = yield self.server.jobgetter.getJobs()
            self.job_queue.extend(jobs)
        except Exception:
            LOGGER.error(format_exc())
        self.getting_jobs = False

    @inlineCallbacks
    def work(self):
        if not self.running:
            return
        r = random()
        self.time_start(r)
        if len(self.job_queue) < 20 and not self.getting_jobs:
            self.getJobs()
        if self.job_queue:
            job = self.job_queue.pop()
            self.time_end(r, "get_jobs")
        else:
            self.time_end(r, "get_jobs", add=.1)
            reactor.callLater(.1, self.work)
            return
        job.start = time.time()
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
            self.server.cassandra.setData(data, job.uuid)
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


