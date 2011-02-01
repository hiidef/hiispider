import txredisapi
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from .base import BaseServer
from ..pagegetter import PageGetter

class CassandraCachingServer(BaseServer):
    def __init__(self,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 cassandra_server=None,
                 cassandra_port=9160,
                 cassandra_keyspace=None,
                 cassandra_http=None,
                 cassandra_headers=None,
                 redis_hosts=None,
                 max_simultaneous_requests=100,
                 max_requests_per_host_per_second=0,
                 max_simultaneous_requests_per_host=0,
                 log_file=None,
                 log_directory=None,
                 log_level="debug",
                 port=8080):
        BaseServer.__init__(
            self,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            max_simultaneous_requests=max_simultaneous_requests,
            max_requests_per_host_per_second=max_requests_per_host_per_second,
            max_simultaneous_requests_per_host=max_simultaneous_requests_per_host,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level)
        self.cassandra_server = cassandra_server
        self.cassandra_port = cassandra_port
        self.cassandra_keyspace = cassandra_keyspace
        self.cassandra_http = cassandra_http
        self.cassandra_headers=cassandra_headers
        self.cassandra_factory = ManagedCassandraClientFactory()
        self.cassandra_client = CassandraClient(self.cassandra_factory, cassandra_keyspace)
        self.setup_redis_client_and_pg(redis_hosts)
        reactor.connectTCP(cassandra_server, cassandra_port, self.cassandra_factory)

    @inlineCallbacks
    def setup_redis_client_and_pg(self, redis_hosts):
        self.redis_client = yield txredisapi.RedisShardingConnection(redis_hosts)
        self.pg = PageGetter(self.cassandra_client,
                             self.cassandra_http,
                             self.cassandra_headers,
                             redis_client=self.redis_client,
                             rq=self.rq)
