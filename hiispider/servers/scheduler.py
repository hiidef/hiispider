from uuid import UUID
import time
import random
from heapq import heappush, heappop
from twisted.internet import reactor, task
from twisted.web import server
from twisted.internet.defer import inlineCallbacks
from txamqp.content import Content
from .base import BaseServer, LOGGER
from .mixins import MySQLMixin, AMQPMixin


from twisted.web.resource import Resource


class SchedulerServer(BaseServer, AMQPMixin, MySQLMixin):

    heap = []
    unscheduled_items = []
    enqueueCallLater = None
    enqueueloop = None
    
    def __init__(self, config, port=None):
        super(SchedulerServer, self).__init__(config)
        self.setupAMQP(config)
        self.setupMySQL(config)
        # HTTP interface
        resource = Resource()
        self.function_resource = Resource()
        resource.putChild("function", self.function_resource)
        if port is None:
            port = config["scheduler_server_port"]
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        # Logging, etc
        self.expose(self.remoteRemoveFromHeap)
        self.expose(self.remoteAddToHeap)
        self.expose(self.enqueueUUID)

    def start(self):
        start_deferred = super(SchedulerServer, self).start()
        start_deferred.addCallback(self._schedulerStart)
        return start_deferred

    @inlineCallbacks
    def _schedulerStart(self, started=None):
        yield self.startJobQueue()
        yield self._loadFromMySQL()
        self.enqueueloop = task.LoopingCall(self.enqueue)
        self.enqueueloop.start(1)
        
    @inlineCallbacks
    def _loadFromMySQL(self):
        data = []
        start = 0
        while len(data) >= 100000 or start == 0:
            sql = """SELECT uuid, type
                     FROM spider_service
                     ORDER BY id LIMIT %s, 100000
                  """ % start
            start += 100000
            data = yield self.mysql.runQuery(sql)
            for row in data:
                self.addToHeap(row["uuid"], row["type"])
        
    @inlineCallbacks
    def shutdown(self):
        self.enqueueloop.stop()
        try:
            self.enqueueCallLater.cancel()
        except:
            pass
        yield self.stopJobQueue()
        yield super(SchedulerServer, self).shutdown()
        
    def enqueue(self):
        now = int(time.time())
        # Compare the heap min timestamp with now().
        # If it's time for the item to be queued, pop it, update the
        # timestamp and add it back to the heap for the next go round.
        queued_items = 0
        if self.amqp_queue_size < 100000:
            LOGGER.debug("%s:%s" % (self.heap[0][0], now))
            while self.heap[0][0] < now and queued_items < 1000:
                job = heappop(self.heap)
                uuid = UUID(bytes=job[1][0])
                if not uuid.hex in self.unscheduled_items:
                    queued_items += 1
                    self.chan.basic_publish(
                        exchange=self.amqp_exchange, 
                        content=Content(job[1][0]))
                    heappush(self.heap, (now + job[1][1], job[1]))
                else:
                    self.unscheduled_items.remove(uuid.hex)
        else:
            LOGGER.critical('AMQP queue is at or beyond max limit (%d/100000)'
                % self.amqp_queue_size)

    def enqueueUUID(self, uuid):
        LOGGER.debug('enqueueUUID: uuid=%s' % uuid)
        self.chan.basic_publish(
            exchange=self.amqp_exchange, 
            content=Content(UUID(uuid).bytes))
        return uuid        
        
    def remoteAddToHeap(self, uuid=None, type=None):
        if uuid and type:
            LOGGER.debug('remoteAddToHeap: uuid=%s, type=%s' % (uuid, type))
            self.addToHeap(uuid, type)
            return {}
        else:
            LOGGER.error('Required parameters are uuid and type')
            return {'error': 'Required parameters are uuid and type'}

    def remoteRemoveFromHeap(self, uuid):
        LOGGER.debug('remoteRemoveFromHeap: uuid=%s' % uuid)
        self.removeFromHeap(uuid)
        
    def addToHeap(self, uuid, type):
        # lookup if type is in the service_mapping, if it is
        # then rewrite type to the proper resource
        if not uuid in self.unscheduled_items:
            if self.service_mapping and type in self.service_mapping:
                LOGGER.info('Remapping resource %s to %s'
                    % (type, self.service_mapping[type]))
                type = self.service_mapping[type]
            try:
                # Make sure the uuid is in bytes
                uuid_bytes = UUID(uuid).bytes
            except ValueError:
                LOGGER.error('Cound not turn UUID into bytes using string: "%s" with type of "%s"'
                    % (uuid, type))
                return
            if type in self.functions and 'interval' in self.functions[type]:
                interval = int(self.functions[type]['interval'])
            else:
                LOGGER.error('Could not find interval for type %s' % type)
                return
            # Enqueue randomly over the interval so it doesn't
            # flood the server at the interval time. only if an interval is defined
            if interval:
                enqueue_time = int(time.time() + random.randint(0, interval))
                # Add a UUID to the heap.
                LOGGER.debug('Adding %s to heap with time %s and interval of %s'
                    % (uuid, enqueue_time, interval))
                heappush(self.heap, (enqueue_time, (uuid_bytes, interval)))
        else:
            LOGGER.info('Unscheduling %s' % uuid)
            self.unscheduled_items.remove(uuid)
            
    def removeFromHeap(self, uuid):
        LOGGER.info('Removing %s from heap' % uuid)
        self.unscheduled_items.append(uuid)
