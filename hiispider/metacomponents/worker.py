from jobexecuter import JobExecuter


class Worker(JobExecuter):

    def __init__(self, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)