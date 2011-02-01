import simplejson
import zlib
from datetime import datetime
import urllib
from uuid import uuid4
from twisted.internet.defer import DeferredList, inlineCallbacks
from twisted.internet import reactor
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient
from telephus.cassandra.ttypes import ColumnPath, ColumnParent, Column
from .base import BaseServer, LOGGER
from ..pagegetter import PageGetter
from ..exceptions import DeleteReservationException
import txredisapi


class CassandraServer(BaseServer):
    def __init__(self,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 cassandra_server=None,
                 cassandra_port=9160,
                 cassandra_keyspace=None,
                 cassandra_cf_content=None,
                 cassandra_content=None,
                 cassandra_content_error='error',
                 cassandra_http=None,
                 cassandra_headers=None,
                 cassandra_error='error',
                 redis_hosts=None,
                 disable_negative_cache=False,
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
        self.cassandra_cf_content = cassandra_cf_content
        self.cassandra_http = cassandra_http
        self.cassandra_headers=cassandra_headers
        self.cassandra_content = cassandra_content
        self.cassandra_content_error = cassandra_content_error
        self.cassandra_factory = ManagedCassandraClientFactory()
        self.cassandra_client = CassandraClient(self.cassandra_factory, cassandra_keyspace)
        self.disable_negative_cache = disable_negative_cache
        self.setup_redis_client_and_pg(redis_hosts)
        reactor.connectTCP(cassandra_server, cassandra_port, self.cassandra_factory)

    @inlineCallbacks
    def setup_redis_client_and_pg(self, redis_hosts):
        self.redis_client = yield txredisapi.RedisShardingConnection(redis_hosts)
        self.pg = PageGetter(
            self.cassandra_client,
            self.cassandra_http,
            self.cassandra_headers,
            redis_client=self.redis_client,
            disable_negative_cache=self.disable_negative_cache,
            rq=self.rq)

    def executeReservation(self, function_name, **kwargs):
        uuid = None
        if not isinstance(function_name, str):
            for key in self.functions:
                if self.functions[key]["function"] == function_name:
                    function_name = key
                    break
        if function_name not in self.functions:
            raise Exception("Function %s does not exist." % function_name)
        function = self.functions[function_name]
        if function["interval"] > 0:
            uuid = uuid4().hex
        d = self.callExposedFunction(
            self.functions[function_name]["function"],
            kwargs,
            function_name,
            uuid=uuid)
        d.addCallback(self._executeReservationCallback, function_name, uuid)
        d.addErrback(self._executeReservationErrback, function_name, uuid)
        return d

    def _executeReservationCallback(self, data, function_name, uuid):
        if not uuid:
            return data
        else:
            return {uuid: data}

    def _executeReservationErrback(self, error, function_name, uuid):
        LOGGER.error("Unable to create reservation for %s:%s, %s.\n" % (function_name, uuid, error))
        return error

    def deleteReservation(self, uuid, function_name="Unknown"):
        LOGGER.info("Deleting reservation %s, %s." % (function_name, uuid))
        d = self.cassandra_client.remove(uuid, self.cassandra_cf_content)
        d.addCallback(self._deleteReservationCallback, function_name, uuid)
        return d

    def _deleteReservationCallback(self, data, function_name, uuid):
        LOGGER.info("Reservation %s, %s successfully deleted." % (function_name, uuid))
        return True

    def _callExposedFunctionCallback(self, data, function_name, uuid):
        data = BaseServer._callExposedFunctionCallback(self, data, function_name, uuid)
        # If we have an place to store the response on Cassandra, do it.
        if uuid is not None and self.cassandra_cf_content is not None and data is not None:
            LOGGER.debug("Putting result for %s, %s on Cassandra." % (function_name, uuid))
            encoded_data = zlib.compress(simplejson.dumps(data))
            d = self.cassandra_client.insert(
                uuid,
                self.cassandra_cf_content,
                encoded_data,
                column=self.cassandra_content)
            d.addErrback(self._exposedFunctionErrback2, data, function_name, uuid)
        return data

    def _callExposedFunctionErrback(self, error, function_name, uuid):
        error = BaseServer._callExposedFunctionErrback(self, error, function_name, uuid)
        try:
            error.raiseException()
        except DeleteReservationException:
            if uuid is not None:
                self.deleteReservation(uuid)
            message = """Error with %s, %s.\n%s
            Reservation deleted at request of the function.""" % (
                function_name,
                uuid,
                error)
            LOGGER.debug(message)
            return
        except:
            pass
        # save error in a error column in the content CF
        # data = {
        #     'msg': error.getErrorMessage(),
        #     'traceback': error.getTraceback(),
        #     'timestamp': datetime.now().isoformat(),
        # }
        # encoded_data = zlib.compress(simplejson.dumps(data))
        # self.cassandra_client.insert(
        #     uuid,
        #     self.cassandra_cf_content,
        #     encoded_data,
        #     column=self.cassandra_content_error)
        return error

    def _exposedFunctionErrback2(self, error, data, function_name, uuid):
        LOGGER.error("Could not put results of %s, %s on Cassandra.\n%s" % (function_name, uuid, error))
        return data
