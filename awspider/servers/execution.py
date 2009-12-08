import cPickle
import time
import pprint
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet import task
from twisted.internet import reactor
from twisted.web import server
from .base import BaseServer, LOGGER
from ..aws import sdb_now, sdb_now_add
from ..resources2 import ExecutionResource

PRETTYPRINTER = pprint.PrettyPrinter(indent=4)

class ExecutionServer(BaseServer):
    
    peers = {}
    peer_uuids = []
    queryloop = None
    coordinateloop = None
    reserved_arguments = [
        "reservation_function_name", 
        "reservation_created", 
        "reservation_next_request", 
        "reservation_error"]
    functions = {}    
    job_limit = 1000
    
    def __init__(self,
                 aws_access_key_id, 
                 aws_secret_access_key, 
                 aws_s3_cache_bucket, 
                 aws_sdb_reservation_domain, 
                 aws_s3_storage_bucket=None,
                 aws_sdb_coordination_domain=None,
                 max_simultaneous_requests=50,
                 max_requests_per_host_per_second=1,
                 max_simultaneous_requests_per_host=5,
                 port=5000, 
                 log_file='executionserver.log',
                 log_directory=None,
                 log_level="debug",
                 name=None,
                 time_offset=None,
                 peer_check_interval=30,
                 reservation_check_interval=30,
                 hammer_prevention=True):
        if name == None:
            name = "AWSpider Execution Server UUID: %s" % self.uuid
        self.hammer_prevention = hammer_prevention
        self.peer_check_interval = int(peer_check_interval)
        self.reservation_check_interval = int(reservation_check_interval)
        resource = ExecutionResource(self)
        self.site_port = reactor.listenTCP(port, server.Site(resource))
        BaseServer.__init__(
            self,
            aws_access_key_id, 
            aws_secret_access_key, 
            aws_s3_cache_bucket=aws_s3_cache_bucket, 
            aws_sdb_reservation_domain=aws_sdb_reservation_domain, 
            aws_s3_storage_bucket=aws_s3_storage_bucket,
            aws_sdb_coordination_domain=aws_sdb_coordination_domain,
            max_simultaneous_requests=max_simultaneous_requests,
            max_requests_per_host_per_second=max_requests_per_host_per_second,
            max_simultaneous_requests_per_host=max_simultaneous_requests_per_host,
            log_file=log_file,
            log_directory=log_directory,
            log_level=log_level,
            name=name,
            time_offset=time_offset,
            port=port)

    def start(self):
        reactor.callWhenRunning(self._start)
        return self.start_deferred

    def _start(self):
        deferreds = []
        deferreds.append(self.getNetworkAddress())
        if self.time_offset is None:
            deferreds.append(self.getTimeOffset())
        d = DeferredList(deferreds, consumeErrors=True)
        d.addCallback(self._startCallback)

    def _startCallback(self, data):
        for row in data:
            if row[0] == False:
                d = self.shutdown()
                d.addCallback(self._startHandleError, row[1])
                return d
        d = BaseServer.start(self)   
        d.addCallback(self._startCallback2)

    def _startCallback2(self, data):
        if self.shutdown_trigger_id is not None:
            self.queryloop = task.LoopingCall(self.query)
            self.queryloop.start(self.reservation_check_interval)
            if self.aws_sdb_coordination_domain is not None:
                self.coordinateloop = task.LoopingCall(self.coordinate)
                self.coordinateloop.start(self.peer_check_interval)  
                d = self.peerCheckRequest()
                if isinstance(d, Deferred):
                    d.addCallback(self._startCallback2)
        
    def shutdown(self):
        deferreds = []
        LOGGER.debug("%s stopping on main HTTP interface." % self.name)
        d = self.site_port.stopListening()
        if isinstance(d, Deferred):
            deferreds.append(d)
        if self.queryloop is not None:
            LOGGER.debug("Stopping query loop.")
            d = self.queryloop.stop()
            if isinstance(d, Deferred):
                deferreds.append(d)
        if self.coordinateloop is not None:
            LOGGER.debug("Stopping coordinating loop.")
            d = self.coordinateloop.stop()
            if isinstance(d, Deferred):
                deferreds.append(d)
            LOGGER.debug( "Removing data from SDB coordination domain.")
            d = self.sdb.delete(self.aws_sdb_coordination_domain, self.uuid )
            d.addCallback(self.peerCheckRequest)
            deferreds.append( d )
        if len(deferreds) > 0:
            d = DeferredList(deferreds)
            d.addCallback(self._shutdownCallback)
            return d
        else:
            return self._shutdownCallback(None)
    
    def _shutdownCallback(self, data):
        return BaseServer.shutdown(self)

    def peerCheckRequest(self, data=None):
        LOGGER.debug("Signaling peers.")
        deferreds = []
        for uuid in self.peers:
            if uuid != self.uuid:
                LOGGER.debug("Signaling %s to check peers." % self.peers[uuid]["uri"])
                d = self.rq.getPage(self.peers[uuid]["uri"] + "/coordinate")
                d.addCallback(self._peerCheckRequestCallback, self.peers[uuid]["uri"])
                deferreds.append(d)
        if len(deferreds) > 0:
            LOGGER.debug("Combinining shutdown signal deferreds.")
            return DeferredList(deferreds)
        return True

    def _peerCheckRequestCallback(self, data, uri):
        LOGGER.debug("Got %s/coordinate." % uri)

    def coordinate(self):
        attributes = {"created":sdb_now(offset=self.time_offset)}
        attributes.update(self.network_information)
        d = self.sdb.putAttributes(
            self.aws_sdb_coordination_domain, 
            self.uuid, 
            attributes, 
            replace=attributes.keys())
        d.addCallback(self._coordinateCallback)
        d.addErrback(self._coordinateErrback)
        
    def _coordinateCallback( self, data ):
        sql = "SELECT public_ip, local_ip, port FROM `%s` WHERE created > '%s'" % (
            self.aws_sdb_coordination_domain, 
            sdb_now_add(self.peer_check_interval * -2, 
            offset=self.time_offset))
        LOGGER.debug( "Querying SimpleDB, \"%s\"" % sql )
        d = self.sdb.select(sql)
        d.addCallback(self._coordinateCallback2)
        d.addErrback(self._coordinateErrback)

    def _coordinateCallback2(self, discovered):
        existing_peers = set(self.peers.keys())
        discovered_peers = set(discovered.keys())
        new_peers = discovered_peers - existing_peers
        old_peers = existing_peers - discovered_peers
        for uuid in old_peers:
            LOGGER.debug("Removing peer %s" % uuid)
            if uuid in self.peers:
                del self.peers[uuid]
        deferreds = []
        for uuid in new_peers:
            if uuid == self.uuid:
                self.peers[uuid] = {
                    "uri":"http://127.0.0.1:%s" % self.port,
                    "local_ip":"127.0.0.1",
                    "port":self.port,
                    "active":True
                }
            else:
                deferreds.append(self.verifyPeer(uuid, discovered[uuid]))
        if len(new_peers) > 0:
            if len(deferreds) > 0:
                d = DeferredList(deferreds, consumeErrors=True)
                d.addCallback(self._coordinateCallback3)
            else:
                self._coordinateCallback3(None) #Just found ourself.
        elif len(old_peers) > 0:
            self._coordinateCallback3(None)
        else:
            pass # No old, no new.

    def _coordinateCallback3( self, data ):
        LOGGER.debug( "Re-organizing peers." )
        for uuid in self.peers:
            if "local_ip" in self.peers[uuid]:
                self.peers[uuid]["uri"] = "http://%s:%s" % (self.peers[uuid]["local_ip"], self.peers[uuid]["port"] )
                self.peers[uuid]["active"] = True
                self.rq.setHostMaxRequestsPerSecond(self.peers[uuid]["local_ip"], 0)
                self.rq.setHostMaxSimultaneousRequests(self.peers[uuid]["local_ip"], 0)
            elif "public_ip" in self.peers[uuid]:
                self.peers[uuid]["uri"] = "http://%s:%s" % (self.peers[uuid]["public_ip"], self.peers[uuid]["port"] )
                self.peers[uuid]["active"] = True
                self.rq.setHostMaxRequestsPerSecond(self.peers[uuid]["public_ip"], 0)
                self.rq.setHostMaxSimultaneousRequests(self.peers[uuid]["public_ip"], 0)
            else:
                LOGGER.error("Peer %s has no local or public IP. This should not happen." % uuid )
        self.peer_uuids = self.peers.keys()
        self.peer_uuids.sort()
        LOGGER.debug("Peers updated to: %s" % self.peers)

    def _coordinateErrback(self, error):
        LOGGER.error( "Could not query SimpleDB for peers: %s" % str(error) )

    def verifyPeer(self, uuid, peer):
        LOGGER.debug( "Verifying peer %s" % uuid )
        deferreds = []
        if "port" in peer:
            port = int(peer["port"][0])
        else:
            port = self.port
        if uuid not in self.peers:
            self.peers[uuid] = {}
        self.peers[uuid]["active"] = False
        self.peers[uuid]["port"] = port
        if "local_ip" in peer:
            local_ip = peer["local_ip"][0]
            local_url = "http://%s:%s/server" % (local_ip, port)
            d = self.pg.getPage(local_url, timeout=5, cache=-1)
            d.addCallback(self._verifyPeerLocalIPCallback, uuid, local_ip, port)
            deferreds.append( d )
        if "public_ip" in peer:
            public_ip = peer["public_ip"][0]
            public_url = "http://%s:%s/server" % (public_ip, port)
            d = self.pg.getPage(public_url, timeout=5, cache=-1)         
            d.addCallback(self._verifyPeerPublicIPCallback, uuid, public_ip, port)
            deferreds.append(d)
        if len(deferreds) > 0:
            d = DeferredList(deferreds, consumeErrors=True)
            return d
        else:
            return None

    def _verifyPeerLocalIPCallback(self, data, uuid, local_ip, port):
        LOGGER.debug("Verified local IP for %s" % uuid)
        self.peers[uuid]["local_ip"] = local_ip

    def _verifyPeerPublicIPCallback(self, data, uuid, public_ip, port):
        LOGGER.debug("Verified public IP for %s" % uuid)
        self.peers[uuid]["public_ip"] = public_ip

    def getPage(self, *args, **kwargs):
        if not self.hammer_prevention or len(self.peer_uuids) == 0:
            return self.pg.getPage(*args, **kwargs)
        else:
            scheme, host, port, path = _parse(args[0])
            peer_key = int(uuid5(NAMESPACE_DNS, host).int % len(self.peer_uuids))
            peer_uuid = self.peer_uuids[peer_key]
            if peer_uuid == self.uuid or self.peers[peer_uuid]["active"] == False:
                return self.pg.getPage(*args, **kwargs)
            else:
                parameters = {}
                parameters["url"] = args[0]
                if "method" in kwargs:
                    parameters["method"] = kwargs["method"]   
                if "postdata" in kwargs: 
                    parameters["postdata"] = urllib.urlencode(kwargs["postdata"])
                if "headers" in kwargs: 
                    parameters["headers"] = urllib.urlencode(kwargs["headers"])
                if "cookies" in kwargs: 
                    parameters["cookies"] = urllib.urlencode(kwargs["cookies"])         
                if "agent" in kwargs:
                    parameters["agent"] = kwargs["agent"]
                if "timeout" in kwargs:
                    parameters["timeout"] = kwargs["timeout"]
                if "followRedirect" in kwargs:
                    parameters["followRedirect"] = kwargs["followRedirect"]
                if "hash_url" in kwargs: 
                    parameters["hash_url"] = kwargs["hash_url"]
                if "cache" in kwargs: 
                    parameters["cache"] = kwargs["cache"]
                if "prioritize" in kwargs: 
                    parameters["prioritize"] = kwargs["prioritize"]
                url = "%s/getpage?%s" % (
                    self.peers[peer_uuid]["uri"], 
                    urllib.urlencode(parameters))
                LOGGER.debug("Re-routing request for %s to %s" % (args[0], url))
                d = self.rq.getPage(url)
                d.addErrback(self._getPageErrback, args, kwargs) 
                return d

    def _getPageErrback( self, error, args, kwargs ):
        LOGGER.error( args[0] + ":" + str(error) )
        return self.pg.getPage(*args, **kwargs)

    def getServerData(self):    
        running_time = time.time() - self.start_time
        cost = (self.sdb.box_usage * .14) * (60*60*24*30.4) / (running_time)
        active_requests_by_host = self.rq.getActiveRequestsByHost()
        pending_requests_by_host = self.rq.getPendingRequestsByHost()
        data = {
            "running_time":running_time,
            "cost":cost,
            "active_requests_by_host":active_requests_by_host,
            "pending_requests_by_host":pending_requests_by_host,
            "active_requests":self.rq.getActive(),
            "pending_requests":self.rq.getPending(),
            "current_timestamp":sdb_now(offset=self.time_offset)
        }
        LOGGER.debug("Got server data:\n%s" % PRETTYPRINTER.pformat(data))
        return data

    def query(self, data=None):
        sql = """SELECT itemName() 
                FROM `%s` 
                WHERE
                reservation_next_request < '%s'
                LIMIT %s""" % (    
                self.aws_sdb_reservation_domain, 
                sdb_now(offset=self.time_offset),
                self.job_limit)
        LOGGER.debug("Querying SimpleDB, \"%s\"" % sql)
        d = self.sdb.select(sql)
        d.addCallback(self._queryCallback)
        d.addErrback(self._queryErrback)

    def _queryErrback(self, error):
        LOGGER.error("Unable to query SimpleDB.\n%s" % error)

    def _queryCallback(self, data):
        deferreds = []
        keys = data.keys()
        if len(keys) > 0:
            i = 0
            while i * 20 < len(keys):
                key_subset = keys[i*20:(i+1)*20]
                i += 1
                sql = """SELECT *
                        FROM `%s` 
                        WHERE
                        itemName() in('%s')
                        """ % (
                        self.aws_sdb_reservation_domain, 
                        "','".join(key_subset))
                LOGGER.debug("Querying SimpleDB, \"%s\"" % sql)
                d = self.sdb.select(sql)
                d.addCallback(self._queryCallback2)
                d.addErrback(self._queryErrback)
                deferreds.append(d)
        if len(keys) >= self.job_limit and len(deferreds) > 0:
            LOGGER.debug("Job limit reached. Querying again.")
            d = DeferredList(deferreds, consumeErrors=True)
            d.addCallback(self.query)
    def _queryCallback2(self, data):
        # Iterate through the reservation data returned from SimpleDB
        for uuid in data:
            kwargs_raw = {}
            reserved_arguments = {}
            # Load attributes into dicts for use by the system or custom functions.
            for key in data[uuid]:
                if key in self.reserved_arguments:
                    reserved_arguments[key] = data[uuid][key][0]
                else:
                    kwargs_raw[key] = data[uuid][key][0]
            # Check for the presence of all required system attributes.
            if "reservation_function_name" not in reserved_arguments:
                LOGGER.error("Reservation %s does not have a function name." % uuid)
                self.deleteReservation(uuid)
                continue
            if "reservation_created" not in reserved_arguments:
                LOGGER.error("Reservation %s, %s does not have a created time." % (function_name, uuid))
                self.deleteReservation( uuid, function_name=function_name )
                continue
            if "reservation_next_request" not in reserved_arguments:
                LOGGER.error("Reservation %s, %s does not have a next request time." % (function_name, uuid))
                self.deleteReservation( uuid, function_name=function_name )
                continue                
            if "reservation_error" not in reserved_arguments:
                LOGGER.error("Reservation %s, %s does not have an error flag." % (function_name, uuid))
                self.deleteReservation( uuid, function_name=function_name )
                continue
            # Check to make sure the custom function is present.
            function_name = reserved_arguments["reservation_function_name"]
            if function_name not in self.functions:
                LOGGER.error("Unable to process function %s for UUID: %s" % (function_name, uuid))
                continue
            # Load custom function.
            if function_name in self.functions:
                exposed_function = self.functions[function_name]
            else:
                LOGGER.error("Could not find function %s." % function_name)
                continue
            # Check for required / optional arguments.
            kwargs = {}
            for key in kwargs_raw:
                if key in exposed_function["required_arguments"]:
                    kwargs[key] = kwargs_raw[key]
                if key in exposed_function["optional_arguments"]:
                    kwargs[key] = kwargs_raw[key]
            has_reqiured_arguments = True
            for key in exposed_function["required_arguments"]:
                if key not in kwargs:
                    has_reqiured_arguments = False
                    LOGGER.error("%s, %s does not have required argument %s." % (function_name, uuid, key))
            if not has_reqiured_arguments:
                continue
            # Call the function.
            LOGGER.debug("Calling %s with args %s" % (function_name, kwargs))
            #reactor.callInThread( self.callExposedFunction, exposed_function["function"], kwargs, function_name, uuid  )
            # Schedule the next request.
#            reservation_next_request_parameters = {
#                "reservation_next_request":sdb_now_add(
#                    exposed_function["interval"], 
#                    offset=self.time_offset)}
#            d = self.sdb.putAttributes(
#                self.aws_sdb_reservation_domain, 
#                uuid, 
#                reservation_next_request_parameters, 
#                replace=["reservation_next_request"])
#            d.addCallback(self._setNextRequestCallback, function_name, uuid)
#            d.addErrback(self._setNextRequestErrback, function_name, uuid)
            
    def _setNextRequestCallback(self, data, function_name, uuid):
        LOGGER.debug("Set next request for %s, %s on on SimpleDB." % (function_name, uuid))

    def _setNextRequestErrback(self, error, function_name, uuid):
        LOGGER.error("Unable to set next request for %s, %s on SimpleDB.\n%s" % (function_name, uuid, error.value))


    