from jobexecuter import JobExecuter
from twisted.internet.defer import inlineCallbacks

class Worker(JobExecuter):

	simultaneous_jobs = 30

	def __init__(self, *args, **kwargs):
		super(Worker, self).__init__(*args, **kwargs)
   
#    def executeJob(self, job):
#    	d = super(Worker, self).executeJob(job)
#
#
#    def executeJobCallback(self, data, job):
#    
#    def executeJobErrback(self, error, job):
#
#    
#
#    def stop(self):
