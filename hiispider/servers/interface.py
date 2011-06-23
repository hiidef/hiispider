import pprint
import urllib
from uuid import uuid4
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web.resource import Resource
from twisted.internet import reactor
from twisted.web import server
from .mixins import JobGetterMixin
from .cassandra import CassandraServer
from .base import LOGGER
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



