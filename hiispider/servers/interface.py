import pprint
import urllib
import zlib
import simplejson
import traceback

from uuid import uuid4
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web.resource import Resource
from twisted.internet import reactor
from twisted.web import server
from .mixins import JobGetterMixin
from .cassandra import CassandraServer
from .base import LOGGER, Job
from ..resources import InterfaceResource


PRETTYPRINTER = pprint.PrettyPrinter(indent=4)


class InterfaceServer(CassandraServer, JobGetterMixin):

    name = "HiiSpider Interface Server UUID: %s" % str(uuid4())

    def __init__(self, config, port=None):
        super(InterfaceServer, self).__init__(config)
        self.setupMySQL(config)
        resource = Resource()
        interface_resource = InterfaceResource(self)
        resource.putChild("interface", interface_resource)
        self.function_resource = Resource()
        resource.putChild("function", self.function_resource)
        if port is None:
            port = config["interface_server_port"]
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        self.scheduler_server = config["scheduler_server"]
        self.scheduler_server_port = config["scheduler_server_port"]

    def start(self):
        start_deferred = super(InterfaceServer, self).start()
        start_deferred.addCallback(self._interfaceStart)
        return start_deferred

    def _interfaceStart(self, success):
        return success

    @inlineCallbacks
    def shutdown(self):
        LOGGER.debug("%s stopping on main HTTP interface." % self.name)
        yield self.site_port.stopListening()
        yield super(InterfaceServer, self).shutdown()

    def enqueueUUID(self, uuid):
        url = 'http://%s:%s/function/schedulerserver/enqueueuuid?%s' % (
            self.scheduler_server,
            self.scheduler_server_port,
            urllib.urlencode({'uuid': uuid}))
        LOGGER.info('Sending UUID to scheduler to be queued: %s' % url)
        return self.rq.getPage(url=url)

    @inlineCallbacks
    def insertData(self, encoded_data, uuid, user_id):
        try:
            if user_id:
                yield self.cassandra_client.insert(str(user_id),
                    self.cassandra_cf_content, encoded_data, column=uuid)
            else:
                yield self.cassandra_client.insert(uuid,
                    self.cassandra_cf_temp_content, encoded_data,
                    column=self.cassandra_content)
        except Exception, e:
            LOGGER.error("Error putting result for uuid %s on Cassandra:\n%s\n" % (uuid, e))
            raise
        returnValue(None)

    @inlineCallbacks
    def executeReservation(self, function_name, **kwargs):
        if not isinstance(function_name, str):
            for key in self.functions:
                if self.functions[key]["function"] == function_name:
                    function_name = key
                    break
        if function_name not in self.functions:
            raise Exception("Function %s does not exist." % function_name)
        function = self.functions[function_name]
        uuid = uuid4().hex if function["interval"] > 0 else None
        user_id = kwargs.get('site_user_id', None)
        data = yield self.executeFunction(function_name, **kwargs)

        if uuid is not None and self.cassandra_cf_content is not None and data is not None:
            LOGGER.debug("Putting result for %s, %s for user_id %s on Cassandra." % (function_name, uuid, user_id))
            encoded_data = zlib.compress(simplejson.dumps(data))
            yield self.insertData(encoded_data, uuid, user_id)
        if not uuid:
            returnValue(data)
        else:
            returnValue({uuid: data})

    @inlineCallbacks
    def deleteReservation(self, job):
        """Delete a reservation."""
        # FIXME: this function is unnecessarily coupled to the job object;
        # only a uuid is needed to delete a reservation
        uuid = job
        if not isinstance(job, basestring):
            uuid = job.uuid
        LOGGER.info('Deleting UUID from spider_service table: %s' % uuid)
        try:
            yield self.mysql.runQuery('DELETE FROM spider_service WHERE uuid=%s', uuid)
        except:
            LOGGER.error(traceback.format_exc())
            raise
        url = 'http://%s:%s/function/schedulerserver/remoteremovefromheap?%s' % (
            self.scheduler_server,
            self.scheduler_server_port,
            urllib.urlencode({'uuid': uuid}))
        LOGGER.info('Sending UUID to scheduler to be dequeued: %s' % url)
        yield self.rq.getPage(url=url)
        LOGGER.info('Deleting UUID from Cassandra: %s' % uuid)
        yield self.cassandra_client.remove(
            uuid,
            self.cassandra_cf_content)
        returnValue({'success':True})
