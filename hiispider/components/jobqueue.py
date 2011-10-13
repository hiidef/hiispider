from queue import Queue


class JobQueue(Queue):

    def __init__(self, server, config, address=None, allow_clients=None, **kwargs):
        kwargs["amqp_vhost"] = config["amqp_jobs_vhost"]
        super(JobQueue, self).__init__(server, config, address=address, allow_clients=allow_clients, **kwargs)
