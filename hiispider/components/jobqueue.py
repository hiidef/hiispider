from queue import Queue


class JobQueue(Queue):

    def __init__(self, server, config, server_mode, **kwargs):
        kwargs["amqp_vhost"] = config["amqp_jobs_vhost"]
        super(JobQueue, self).__init__(server, config, server_mode, **kwargs)
