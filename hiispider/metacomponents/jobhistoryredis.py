from ..components import Redis


class JobHistoryRedis(Redis):

    def __init__(self, server, config, address=None, **kwargs):
        kwargs["redis_hosts"] = [config["jobhistory"]["host"]]
        super(JobHistoryRedis, self).__init__(server, config, address=None, **kwargs)