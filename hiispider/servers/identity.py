from uuid import uuid4
from pprint import pformat
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient
from twisted.web.resource import Resource
from twisted.internet import reactor, task
from twisted.web import server
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList
import logging
from .mixins import MySQLMixin, IdentityQueueMixin
from .base import BaseServer
from telephus.cassandra.c08.ttypes import NotFoundException


logger = logging.getLogger(__name__)


class IdentityServer(BaseServer, MySQLMixin, IdentityQueueMixin):

    name = "HiiSpider Identity Server UUID: %s" % str(uuid4())
    simultaneous_jobs = 50
    active_jobs = 0
    updating_connections = {}
    updating_identities = {}
    connections_queue = []
    connectionsloop = None
    dequeueloop = None
    queue_requests = 0

    def __init__(self, config, port=None):
        super(IdentityServer, self).__init__(config)
        self.plugin_mapping = config["plugin_mapping"]
        self.setupMySQL(config)
        self.setupIdentityQueue(config)
        self.cassandra_cf_identity = config["cassandra_cf_identity"]
        self.cassandra_cf_connections = config["cassandra_cf_connections"]
        self.cassandra_cf_recommendations = config["cassandra_cf_recommendations"]
        self.cassandra_cf_reverse_recommendations = config["cassandra_cf_reverse_recommendations"]
        factory = ManagedCassandraClientFactory(config["cassandra_keyspace"])
        self.cassandra_client = CassandraClient(factory)
        reactor.connectTCP(
            config["cassandra_server"],
            config.get("cassandra_port", 9160),
            factory)
        resource = Resource()
        self.function_resource = Resource()
        resource.putChild("function", self.function_resource)
        if port is None:
            port = config["identity_server_port"]
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        self.expose(self.updateConnections)
        self.expose(self.updateAllConnections)
        self.expose(self.updateAllIdentities)
        self.expose(self.getRecommendations)
        self.expose(self.getReverseRecommendations)
        self.expose(self.updateIdentity)
        
    def start(self):
        start_deferred = super(IdentityServer, self).start()
        start_deferred.addCallback(self._identityStart)
        return start_deferred

    @inlineCallbacks
    def _identityStart(self, started=False):
        yield self.startIdentityQueue()
        self.connectionsloop = task.LoopingCall(self.findConnections)
        self.connectionsloop.start(0.2)
        self.dequeueloop = task.LoopingCall(self.dequeue)
        self.dequeueloop.start(1)

    @inlineCallbacks
    def shutdown(self):
        self.connectionsloop.stop()
        self.dequeueloop.stop()
        yield self.stopIdentityQueue()
        yield super(IdentityServer, self).shutdown()

    def updateUser(self, user_id):
        reactor.callLater(0, self._updateUser, user_id)
        return {"success":True, "message":"User update started."}
    
    @inlineCallbacks
    def _updateUser(self, user_id):
        sql = """SELECT type FROM content_account WHERE user_id=%s"""
        data = yield self.mysql.runQuery(sql, int(user_id))
        deferreds = [self._updateIdentity(str(user_id), x["type"]) for x in data if "custom" not in x["type"]]
        results = yield DeferredList(deferreds, consumeErrors=True)
        for result in results:
            if not result[0]:
                raise result[1]
        deferreds = [self._updateConnections(str(user_id), x["type"]) for x in data if "custom" not in x["type"]]
        results = yield DeferredList(deferreds, consumeErrors=True)        
        for result in results:
            if not result[0]:
                raise result[1]

    def updateAllIdentities(self, service_name):
        if self.updating_identities.get(service_name, False):
            return {"success":False, "message":"Already updating %s" % service_name}
        else:
            reactor.callLater(0, self._updateAllIdentities, service_name)
            return {"success":True, "message":"Update all identities started."}
             
    @inlineCallbacks
    def _updateAllIdentities(self, service_name):
        self.updating_identities[service_name] = True
        sql = """SELECT user_id 
        FROM content_%(service_name)saccount 
        INNER JOIN content_account 
            ON content_%(service_name)saccount.account_id = content_account.id
        LIMIT %%s, %%s
        """ % {"service_name":service_name}
        start = 0
        step = 100
        data = yield self.mysql.runQuery(sql, (start, step))
        while data:
            d = [self._updateIdentity(str(x["user_id"]), service_name) for x in data]
            results = yield DeferredList(d, consumeErrors=True)
            for result in results:
                if not result[0]:
                    raise result[1]
            start += step
            data = yield self.mysql.runQuery(sql, (start, step))
        self.updating_connections[service_name] = False
    
    def updateAllConnections(self, service_name):
        if self.updating_connections.get(service_name, False):
            return {"success":False, "message":"Already updating %s" % service_name}
        else:
            reactor.callLater(0, self._updateAllConnections, service_name)
            return {"success":True, "message":"Update all connections started."}

    @inlineCallbacks
    def _updateAllConnections(self, service_name):
        self.updating_connections[service_name] = True
        sql = """SELECT user_id 
        FROM content_%(service_name)saccount 
        INNER JOIN content_account 
            ON content_%(service_name)saccount.account_id = content_account.id
        LIMIT %%s, %%s
        """ % {"service_name":service_name}
        start = 0
        step = 40
        data = yield self.mysql.runQuery(sql, (start, step))
        while data:
            d = [self._updateConnections(str(x["user_id"]), service_name) for x in data]
            results = yield DeferredList(d, consumeErrors=True)
            for result in results:
                if not result[0]:
                    raise result[1]
            start += step
            data = yield self.mysql.runQuery(sql, (start, step))
        self.updating_connections[service_name] = False
        returnValue({"success":True})

    @inlineCallbacks
    def _accountData(self, user_id, service_name):
        sql = """SELECT content_%(service_name)saccount.* 
        FROM content_%(service_name)saccount 
        INNER JOIN content_account 
            ON content_%(service_name)saccount.account_id = content_account.id
        WHERE content_account.user_id = %%s""" % {"service_name":service_name}
        try:
            data = yield self.mysql.runQuery(sql, user_id)
        except Exception, e:
            message = "Could not find service %s:%s, %s" % (
                service_name, 
                user_id, 
                sql)
            logger.error(message)
            raise
        if len(data) == 0: # No results?
            message = "Could not find service %s:%s" % (service_name, user_id)
            logger.error(message)
            raise Exception(message)
        if service_name in self.inverted_args_mapping:
            mapping = self.inverted_args_mapping[service_name]
            for kwargs in data:
                for key, value in mapping.iteritems():
                    if value in kwargs:
                        kwargs[key] = kwargs.pop(value)
        returnValue(data)  
              
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

    def updateConnections(self, user_id, service_name):
        reactor.callLater(0, self._updateConnections, user_id, service_name)
        return {"success":True, "message":"Update identity started."}        

    @inlineCallbacks
    def _updateConnections(self, user_id, service_name):
        logger.debug("Updating %s for user %s." % (service_name, user_id))
        data = yield self._accountData(user_id, service_name)
        ids = []
        for kwargs in data:
            function_key = "%s/_getconnections" % self.plugin_mapping.get(service_name, service_name)
            try:
                account_ids = yield self.executeFunction(function_key, **kwargs)
            except NotImplementedError:
                logger.info("%s not implemented." % function_key)
                return                
            except Exception, e:
                logger.error(e.message)
                return
            ids.extend(account_ids)
        data = yield self.cassandra_client.get_slice(
            key = user_id,
            column_family=self.cassandra_cf_connections,
            start=service_name,
            finish=service_name + chr(0xff))
        ids = set(ids)
        old_ids = dict([(x.column.name.split("|").pop(), x.column.value) for x in data])
        new_ids = ids - set(old_ids)
        obsolete_ids = set(old_ids) - ids
        for service_id in obsolete_ids:
            try:
                logger.debug("Removing %s|%s from connections CF." % (service_name, service_id))
                yield self.cassandra_client.remove(
                    key=user_id,
                    column_family=self.cassandra_cf_connections,
                    column="%s|%s" % (service_name, service_id))
                logger.debug("Decrementing %s:%s." % (user_id, old_ids[service_id]))
                yield DeferredList([
                    self.client.add(
                        key=user_id, 
                        column_family=self.cassandra_cf_recommendations,
                        value=-1, 
                        column=old_ids[service_id]),
                    self.client.add(
                        key=old_ids[service_id], 
                        column_family=self.cassandra_cf_reverse_recommendations,
                        value=-1, 
                        column=user_id)])
            except Exception, e:
                logger.error(e.message)
        mapped_new_ids = {}
        for chunk in list(self.chunks(list(new_ids), 50)):
            data = yield self.cassandra_client.multiget(
                keys = ["%s|%s" % (service_name, x) for x in chunk],
                column_family=self.cassandra_cf_identity,
                column="user_id")
            for key in data:
                if data[key]:
                    mapped_new_ids[key] = data[key][0].column.value
        if not mapped_new_ids:
            return
        logger.debug("Batch inserting: %s" % pformat(mapped_new_ids))
        yield self.cassandra_client.batch_insert(
            key=user_id,
            column_family=self.cassandra_cf_connections,
            mapping=mapped_new_ids)
        folowee_ids = mapped_new_ids.values()
        for chunk in list(self.chunks(folowee_ids, 10)):
            deferreds = []
            for followee_id in chunk:
                logger.info("Incrementing %s:%s" % (user_id, followee_id))
                deferreds.append(self.cassandra_client.add(
                    key=user_id, 
                    column_family=self.cassandra_cf_recommendations,
                    value=1, 
                    column=followee_id))
                deferreds.append(self.cassandra_client.add(
                    key=followee_id, 
                    column_family=self.cassandra_cf_reverse_recommendations,
                    value=1, 
                    column=user_id))
            yield DeferredList(deferreds)

    @inlineCallbacks
    def shutdown(self):
        logger.debug("%s stopping on main HTTP interface." % self.name)
        yield self.site_port.stopListening()
        yield super(IdentityServer, self).shutdown()

    @inlineCallbacks
    def getRecommendations(self, user_id):
        data = yield self.cassandra_client.get_slice(
            key=user_id, 
            column_family=self.cassandra_cf_recommendations)
        returnValue(sorted(
            [(int(x.counter_column.name), int(x.counter_column.value)) for x in data], 
            key=lambda x:x[1],
            reverse=True))

    @inlineCallbacks
    def getReverseRecommendations(self, user_id):
        data = yield self.cassandra_client.get_slice(
            key=user_id, 
            column_family=self.cassandra_cf_reverse_recommendations)
        returnValue(sorted(
            [(int(x.counter_column.name), int(x.counter_column.value)) for x in data], 
            key=lambda x:x[1], 
            reverse=True))
    
    def dequeue(self):
        while len(self.connections_queue) + self.queue_requests <= self.amqp_prefetch_count:
            self.queue_requests += 1
            logger.debug('Fetching from queue, %s queue requests.' % self.queue_requests)
            self.dequeue_item()

    @inlineCallbacks
    def dequeue_item(self):
        try:
            user_id = yield self.getIdentityUserID()
            logger.debug('Got user_id: %s' % user_id)
        except Exception, e:
            # if we've started shutting down, ignore this error
            if self.shutdown_trigger_id is None:
                return
            logger.error('Dequeue Error: %s' % e)
            return
        self.queue_requests -= 1
        self.connections_queue.append(user_id)

    def findConnections(self):
        while len(self.connections_queue) > 0 and self.active_jobs < self.simultaneous_jobs:
            self.active_jobs += 1
            d = self._updateUser(self.connections_queue.pop(0))
            d.addCallback(self._findConnectionsCallback)
            d.addErrback(self._findConnectionsErrback)

    def _findConnectionsCallback(self, data):
        self.active_jobs -= 1
    
    def _findConnectionsErrback(self, error):
        self.active_jobs -= 1
        logger.error(str(error))