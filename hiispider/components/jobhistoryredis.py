from .redis import Redis
from .base import shared

class JobHistoryRedis(Redis):

    enabled = False

    def __init__(self, server, config, server_mode, **kwargs):
        conf = config.get('jobhistory', {})
        if conf and conf.get('enabled', False):
            self.enabled = True
            kwargs["redis_hosts"] = [conf["host"]]
        else:
            kwargs["redis_hosts"] = []
        super(JobHistoryRedis, self).__init__(server, config, server_mode, **kwargs)

    @shared
    def save(self, job, success):
        if not self.enabled or not job.uuid:
            return
        key = "job:%s:%s" % (job.uuid, 'good' if success else 'bad')
        self.client._send('LPUSH', key, time.time())
        self.client._send('LTRIM', key, 0, 9)

