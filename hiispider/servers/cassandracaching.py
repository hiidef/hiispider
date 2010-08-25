import cjson
import zlib
from datetime import datetime
import urllib
from uuid import uuid4
from twisted.internet.defer import DeferredList
from twisted.internet import reactor
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient
from telephus.cassandra.ttypes import ColumnPath, ColumnParent, Column
from .base import BaseServer, LOGGER
from ..pagegetter import PageGetter
from ..exceptions import DeleteReservationException

class CassandraCachingServer(BaseServer):
    
    def __init__(self,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 cassandra_server=None,
                 cassandra_port=9160,
                 cassandra_keyspace=None, 
                 cassandra_cf_cache=None,
                 cassandra_http=None,
                 cassandra_headers=None,
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
        self.cassandra_cf_cache = cassandra_cf_cache
        self.cassandra_http = cassandra_http
        self.cassandra_headers=cassandra_headers
        self.cassandra_factory = ManagedCassandraClientFactory()
        self.cassandra_client = CassandraClient(self.cassandra_factory, cassandra_keyspace)
        reactor.connectTCP(cassandra_server, cassandra_port, self.cassandra_factory)
        self.pg = PageGetter(
            self.cassandra_client, 
            self.cassandra_cf_cache,
            self.cassandra_http,
            self.cassandra_headers,
            rq=self.rq)
        
