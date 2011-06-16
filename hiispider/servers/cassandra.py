import simplejson
import zlib
from datetime import datetime
import urllib
from uuid import uuid4
import pprint
from twisted.internet.defer import DeferredList, inlineCallbacks, returnValue
from twisted.internet import reactor
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient
from telephus.cassandra.ttypes import ColumnPath, ColumnParent, Column, ConsistencyLevel
from .base import BaseServer, LOGGER
from ..pagegetter import PageGetter
from ..exceptions import DeleteReservationException
import txredisapi

PP = pprint.PrettyPrinter(indent=4)

class CassandraServer(BaseServer):

    def __init__(self, config):
        super(CassandraServer, self).__init__(config)
        self.cassandra_server = config["cassandra_server"]
        self.cassandra_port = config["cassandra_port"]
        self.cassandra_keyspace = config["cassandra_keyspace"]
        self.cassandra_cf_content = config["cassandra_cf_content"]
        # Cassandra Clients & Factorys
        self.cassandra_factory = ManagedCassandraClientFactory(self.cassandra_keyspace)
        self.cassandra_client = CassandraClient(self.cassandra_factory)
        # Negative Cache
        self.disable_negative_cache = config["disable_negative_cache"]
        # Redis
        self.redis_hosts = config["redis_hosts"]
        # Casasndra reactor
        reactor.connectTCP(config["cassandra_server"], config["cassandra_port"], self.cassandra_factory)
        
    def start(self):
        start_deferred = super(CassandraServer, self).start()
        start_deferred.addCallback(self._cassandraStart)
        return start_deferred

    @inlineCallbacks
    def _cassandraStart(self, started=False):
        LOGGER.debug("Starting Cassandra components.")
        try:
            self.redis_client = yield txredisapi.RedisShardingConnection(self.redis_hosts)
        except Exception, e:
            LOGGER.error("Could not connect to Redis: %s" % e)
            self.shutdown()
            raise Exception("Could not connect to Redis.")
        self.pg = PageGetter(
            self.cassandra_client,
            redis_client=self.redis_client,
            disable_negative_cache=self.disable_negative_cache,
            rq=self.rq)
        returnValue(True)
    
    def shutdown(self):
        # Shutdown things here.
        return super(CassandraServer, self).shutdown()
    
    @inlineCallbacks
    def executeJob(self, job):
        user_id = job.user_account["user_id"]
        new_data = yield super(CassandraServer, self).executeJob(job)
        if new_data is None:
            return
        delta_func = self.functions[job.function_name]["delta"]
        if delta_func is not None:
            old_data = yield self.getData(user_id, job.uuid)
            delta = delta_func(new_data, old_data)
            LOGGER.error("Got delta: %s" % str(delta))
            # Temporary debugging.
            old_data_file = open("/Users/johnwehr/Projects/flavors_spider/delta/%s.old" % job.uuid, 'w')
            new_data_file = open("/Users/johnwehr/Projects/flavors_spider/delta/%s.new" % job.uuid, 'w')
            delta_data_file = open("/Users/johnwehr/Projects/flavors_spider/delta/%s.delta" % job.uuid, 'w')
            old_data_file.write(PP.pformat(old_data))
            old_data_file.close()
            new_data_file.write(PP.pformat(new_data))
            new_data_file.close()
            delta_data_file.write(PP.pformat(delta))
            delta_data_file.close()
        yield self.cassandra_client.insert(
            str(user_id),
            self.cassandra_cf_content,
            zlib.compress(simplejson.dumps(new_data)),
            column=job.uuid)
        returnValue(new_data)
        
    def executeReservation(self, function_name, **kwargs):
        uuid = None
        site_user_id = None
        if 'site_user_id' in kwargs:
            site_user_id = kwargs['site_user_id']
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
        d.addCallback(self._executeReservationCallback, function_name, uuid, user_id)
        d.addErrback(self._executeReservationErrback, function_name, uuid)
        return d

    def _executeReservationCallback(self, data, function_name, uuid, user_id):
        # If we have an place to store the response on Cassandra, do it.
        if uuid is not None and self.cassandra_cf_content is not None and data is not None:
            LOGGER.debug("Putting result for %s, %s for user_id %s on Cassandra." % (function_name, uuid, user_id))
            encoded_data = zlib.compress(simplejson.dumps(data))
            if user_id:
                d = self.cassandra_client.insert(
                    str(user_id),
                    self.cassandra_cf_content,
                    encoded_data,
                    column=uuid,
                    consistency=ConsistencyLevel.QUORUM)
            else:
                d = self.cassandra_client.insert(
                    uuid,
                    self.cassandra_cf_temp_content,
                    encoded_data,
                    column=self.cassandra_content,
                    consistency=ConsistencyLevel.QUORUM)
            d.addErrback(self._exposedFunctionErrback2, data, function_name, uuid)
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
        return error

    def _exposedFunctionErrback2(self, error, data, function_name, uuid):
        LOGGER.error("Could not put results of %s, %s on Cassandra.\n%s" % (function_name, uuid, error))
        return data
    
    @inlineCallbacks    
    def getData(self, user_id, uuid):
        data = yield self.cassandra_client.get(
            key=str(user_id),
            column_family=self.cassandra_cf_content,
            column=uuid)
        returnValue(simplejson.loads(zlib.decompress(data.column.value)))
        