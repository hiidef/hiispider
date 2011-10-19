from queue import Queue


class IdentityQueue(Queue):

    def __init__(self, server, config, server_mode, **kwargs):
        kwargs["amqp_vhost"] = config["amqp_identity_vhost"]
        super(IdentityQueue, self).__init__(server, config, server_mode, **kwargs)