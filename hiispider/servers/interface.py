import pprint
import urllib
import zlib
import simplejson
import traceback
import logging

from uuid import uuid4
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web.resource import Resource
from twisted.internet import reactor
from twisted.web import server
from .mixins import JobGetterMixin
from .cassandra import CassandraServer
from .base import Job
from ..resources import InterfaceResource


PRETTYPRINTER = pprint.PrettyPrinter(indent=4)
logger = logging.getLogger(__name__)


class InterfaceServer(CassandraServer):

    disable_negative_cache = True
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
        self.cassandra_cf_identity = config["cassandra_cf_identity"]
        # setup manhole
        manhole_namespace = {
            'service': self,
            'globals': globals(),
        }
        reactor.listenTCP(config["manhole_interface_port"], self.getManholeFactory(manhole_namespace, admin=config["manhole_password"]))
        # delta debugging
        if self.delta_debug:
            self.expose(self.regenerate_delta)
            self.expose(self.regenerate_deltas)
            self.expose(self.updateIdentity)

    def start(self):
        start_deferred = super(InterfaceServer, self).start()
        start_deferred.addCallback(self._interfaceStart)
        return start_deferred

    def _interfaceStart(self, success):
        return success

    @inlineCallbacks
    def shutdown(self):
        logger.debug("%s stopping on main HTTP interface." % self.name)
        yield self.site_port.stopListening()
        yield super(InterfaceServer, self).shutdown()

    def enqueueUUID(self, uuid):
        url = 'http://%s:%s/function/schedulerserver/enqueuejobuuid?%s' % (
            self.scheduler_server,
            self.scheduler_server_port,
            urllib.urlencode({'uuid': uuid}))
        logger.info('Sending UUID to scheduler to be queued: %s' % url)
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
                    column=self.cassandra_cf_content)
        except Exception, e:
            logger.error("Error putting result for uuid %s on Cassandra:\n%s\n" % (uuid, e))
            raise
        returnValue(None)

    @inlineCallbacks
    def executeExposedFunction(self, function_name, **kwargs):
        function = self.functions[function_name]
        data = yield super(InterfaceServer, self).executeExposedFunction(function_name, **kwargs)
        uuid = uuid4().hex if function["interval"] > 0 else None
        user_id = kwargs.get('site_user_id', None)
        if uuid is not None and self.cassandra_cf_content is not None and data is not None:
            logger.debug("Putting result for %s, %s for user_id %s on Cassandra." % (function_name, uuid, user_id))
            encoded_data = zlib.compress(simplejson.dumps(data))
            yield self.insertData(encoded_data, uuid, user_id)
        if not uuid:
            returnValue(data)
        else:
            returnValue({uuid: data})
    
    def updateIdentity(self, user_id, service_name):
        reactor.callLater(0, self._updateIdentity, user_id, service_name)
        return {"success":True, "message":"Update identity started."}
    
    @inlineCallbacks
    def _updateIdentity(self, user_id, service_name):
        data = yield self._accountData(user_id, service_name)
        for kwargs in data:
            function_key = "%s/_getidentity" % self.plugin_mapping.get(service_name, service_name)
            try:
                service_id = yield self.executeFunction(function_key, **kwargs)
            except NotImplementedError:
                logger.info("%s not implemented." % function_key)
                return 
            yield self.cassandra_client.insert(
                "%s|%s" % (service_name, service_id), 
                self.cassandra_cf_identity,
                user_id, 
                column="user_id")

