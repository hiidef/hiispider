import pprint
import urllib
from uuid import uuid4
from MySQLdb.cursors import DictCursor
from twisted.enterprise import adbapi
from twisted.internet.defer import Deferred, DeferredList, maybeDeferred, inlineCallbacks
from twisted.internet.threads import deferToThread
from twisted.web.resource import Resource
from twisted.internet import reactor
from twisted.web import server
from .mixins import JobGetterMixin
from .cassandra import CassandraServer
from .base import LOGGER, SmartConnectionPool
from ..resources import InterfaceResource
from ..evaluateboolean import evaluateBoolean
import zlib
from ..amqp import amqp as AMQP


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
    
    def start(self):
        start_deferred = super(InterfaceServer, self).start()
        start_deferred.addCallback(self._interfaceStart)
        return start_deferred
    
    @inlineCallbacks
    def _interfaceStart(self):
        yield self.startJobGetter()
    
    @inlineCallbacks
    def shutdown(self):
        LOGGER.debug("%s stopping on main HTTP interface." % self.name)
        yield self.site_port.stopListening()
        yield self.stopJobGetter()
        yield super(InterfaceServer, self).shutdown()
        
    def enqueueUUID(self, uuid):
        if uuid and self.scheduler_server and self.scheduler_server_port:
            parameters = {'uuid': uuid}
            query_string = urllib.urlencode(parameters)
            url = 'http://%s:%s/function/schedulerserver/enqueueuuid?%s' % (self.scheduler_server, self.scheduler_server_port, query_string)
            LOGGER.info('Sending UUID to scheduler to be queued: %s' % url)
            d = self.rq.getPage(url=url)
            d.addCallback(self._enqueueCallback, uuid)
            d.addErrback(self._enqueueErrback, uuid)
            return d
        return None
    
    def _enqueueCallback(self, data, uuid):
        LOGGER.info("%s is added to spider queue." % uuid)
    
    def _enqueueErrback(self, error, uuid):
        LOGGER.info("Could not enqueue %s." % uuid)
            
