from jobexecuter import JobExecuter


class Worker(JobExecuter):

    def __init__(self, *args, **kwargs):
        super(JobExecuter, self).__init__(*args, **kwargs)