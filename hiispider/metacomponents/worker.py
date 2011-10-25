from jobexecuter import JobExecuter
from twisted.internet.defer import inlineCallbacks
from copy import copy
import logging
from twisted.internet import reactor
from traceback import format_exc
from ..sleep import Sleep
from random import random
import time
from twisted.internet import task
from collections import defaultdict
from pprint import pformat

LOGGER = logging.getLogger(__name__)  

class Worker(JobExecuter):

    active_workers = 0
    simultaneous_jobs = 25
    jobs = set([])
    job_speed_start = time.time()
    job_speed_report_loop = None
    timer = defaultdict(lambda:0)
    timer_starts = {}

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
        self._job_speed_report(pformat(self.timer))

    def _job_speed_report(self):
        jps = self.jobs_complete / (time.time() - self.job_speed_start)
        fps = self.job_failures / (time.time() - self.job_speed_start)
        LOGGER.info("%s jobs per second, %s failures per second." % (jps, fps))
        LOGGER.info()
        self.job_speed_start = time.time()
        self.jobs_complete = 0
        self.job_failures = 0

    def shutdown(self):
        if self.job_speed_report_loop:
            self.job_speed_report_loop.stop()

    def time_start(self, task_id):
        self.timer_starts[task_id] = time.time()

    def time_end(self, task_id, task):
        self.timer[task] += time.time() - self.timer_starts[task_id]
        del self.timer_starts[task_id]

    @inlineCallbacks
    def work(self):
        if not self.running:
            return
        try:
            r = random()
            self.time_start(r)
            job = yield self.server.jobgetter.getJob()
            self.time_end(r, "job_wait")
        except:
            self.time_end(r, "job_wait")
            LOGGER.error(format_exc())
            reactor.callLater(0, self.work)
            return
        self.jobs.add(job)
        try:
            self.time_start(job.uuid)
            data = yield self.executeJob(job)
            self.time_end(job.uuid, job.function_name)
        except:
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
            except:
                self.time_end(job.uuid, "%s.delta" % job.function_name)
                LOGGER.error(format_exc())
        try:
            self.time_start(job.uuid)
            self.server.cassandra.setData(data, job)
            self.time_end(job.uuid, "%s.cassandra" % job.function_name)
        except:
            self.time_end(job.uuid, "%s.cassandra" % job.function_name)
            LOGGER.error("Job: %s\n%s" % (job.function_name, format_exc()))
        self.jobs.remove(job)
        reactor.callLater(0, self.work)


    def _deltas(self):
        if self.delta_enabled:
            return random.random() <= self.delta_sample_rate
        return False

    deltas = property(_deltas)

