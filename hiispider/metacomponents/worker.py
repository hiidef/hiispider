from jobexecuter import JobExecuter
from twisted.internet.defer import inlineCallbacks
from copy import copy
import logging
from twisted.internet import reactor
from traceback import format_exc

LOGGER = logging.getLogger(__name__)

class Worker(JobExecuter):

    simultaneous_jobs = 30

    def __init__(self, server, config, address=None, **kwargs):
        super(Worker, self).__init__(server, config, address=address)
        config = copy(config)
        config.update(kwargs)
        self.delta_enabled = config.get('delta_enabled', False)
        self.delta_sample_rate = config.get('delta_sample_rate', 1.0)

    def start(self):
        super(Worker, self).start()
        #for i in range(0, self.simultaneous_jobs):
        #    self.work()

    def shutdown(self):
        self.running = False

    @inlineCallbacks
    def work(self):
        if not self._running:
            return
        try:
            job = yield self.server.jobgetter.getJob()
            data = yield self.executeJob(job)
        except Exception, e:
            LOGGER.error(format_exc())
            reactor.callLater(0, self.work)
            return
        if data is None:
            reactor.callLater(0, self.work)
            return
        if self.deltas:
            try:
                yield self.generate_deltas(data, job)
            except:
                LOGGER.error(format_exc())
        try:
            self.setData(data, job)
        except:
            LOGGER.error(format_exc())
        reactor.callLater(0, self.work)
 
    def _deltas(self):
        if self.delta_enabled:
            return random.random() <= self.delta_sample_rate
        return False

    deltas = property(_deltas)

