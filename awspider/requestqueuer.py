import urllib
import time
from twisted.internet.defer import Deferred
from twisted.internet import reactor, ssl
from twisted.web.client import HTTPClientFactory, _parse
import dateutil.parser
from .unicodeconverter import convertToUTF8


class RequestQueuer(object):
    
    """
    HTTP Request Queuer
    """
    
    # Dictionary of lists of pending requests, by host
    pending_reqs = {}
    # Dictonary of timestamps - via time() - of last requests, by host
    last_req = {}
    # Dictonary of integer counts of active requests, by host
    active_reqs = {}
    # Dictionary of user specified minimum request intervals, by host
    min_req_interval_per_hosts = {}
    # Dictionary of user specified maximum simultaneous requests, by host
    max_simul_reqs_per_hosts = {}

    def __init__(self, max_simultaneous_requests=50,
                 max_requests_per_host_per_second=1,
                 max_simultaneous_requests_per_host=5): 
        """
        Set the maximum number of simultaneous requests for a particular host.
        
        **Keyword arguments:**
          * *max_simultaneous_requests* -- Maximum number of simultaneous
            requests RequestQueuer should make across all hosts. (Default 50)
          * *max_requests_per_host_per_second* -- Maximum number of requests 
            per host per second. If set to 1, RequestQueuer will not make more 
            than 1 request per second to a host. If set to 0, RequestQueuer 
            will not limit the request rate. Can be overridden for an 
            individual host using ``setHostMaxRequestsPerSecond()`` (Default 1)
          * *max_simultaneous_requests_per_host* -- Maximum number of 
            simultaneous requests per host. If set to 1, RequestQueuer will
            not make more than 1 simultaneous request per host. If set to 0,
            RequestQueuer will not limit the number of simultaneous requests.
            Can be overridden for an individual host using 
            ``setHostMaxSimultaneousRequests()`` (Default 5)
  
        """
        self.max_simul_reqs = int(max_simultaneous_requests)
        # self.min_req_interval_per_host is the global minimum request
        # interval. Can be overridden by self.min_req_interval_per_hosts[].
        max_req_per_host_per_sec = float(max_requests_per_host_per_second)
        self.min_req_interval_per_host = 1.0 / max_req_per_host_per_sec
        # self.max_simul_reqs_per_host is the global maximum simultaneous 
        # request count. Can be overridden by self.max_simul_reqs_per_hosts[].
        self.max_simul_reqs_per_host = int(max_simultaneous_requests_per_host)

    def getPending(self):
        """
        Return the number of pending requests.
        """
        return sum([len(x) for x in self.pending_reqs.values()])

    def getActive(self):
        """
        Return the number of active requests.
        """
        return sum(self.active_reqs.values())
    
    def getActiveRequestsByHost(self):
        """
        Return a dictionary of the number of active requests by host.
        """
        return self.active_reqs
        
    def getPendingRequestsByHost(self):
        """
        Return a dictionary of the number of pending requests by host.
        """
        reqs = [(x[0], len(x[1])) for x in self.pending_reqs.items()]
        return dict(reqs)

    def setHostMaxRequestsPerSecond(self, host, max_requests_per_second):
        """
        Set the maximum number of requests per second for a particular host.
        
        **Arguments:**
         * *host* -- Hostname. (Example, ``"google.com"``)
         * *max_requests_per_second* -- Maximum number of requests to the
           host per second. If set to 1, RequestQueuer will not make more 
           than 1 request per second to the host. If set to 0, RequestQueuer 
           will not limit the request rate to the host.
        """        
        if max_requests_per_second == 0:
            self.min_req_interval_per_hosts[host] = 0
        else:
            min_req_interval = 1.0 / float(max_requests_per_second)
            self.min_req_interval_per_hosts[host] = min_req_interval

    def setHostMaxSimultaneousRequests(self, host, max_simultaneous_requests):
        """
        Set the maximum number of simultaneous requests for a particular host.
        
        **Arguments:**
         * *host* -- Hostname. (Example, ``"google.com"``)
         * *max_simultaneous_requests* -- Maximum number of simultaneous
           requests to the host. If set to 1, RequestQueuer will not make
           more than 1 simultaneous request to the host. If set to 0,
           RequestQueuer will not limit the number of simultaneous requests.
        """
        if max_simultaneous_requests == 0:
            self.max_simul_reqs_per_hosts[host] = self.max_simul_reqs
        else:
            self.max_simul_reqs_per_hosts[host] = max_simultaneous_requests

    def getPage(self, 
                url, 
                last_modified=None, 
                etag=None, 
                method='GET', 
                postdata=None, 
                headers=None, 
                agent="RequestQueuer", 
                timeout=60, 
                cookies=None, 
                follow_redirect=True, 
                prioritize=False
                ):
        """
        Make an HTTP Request.

        **Arguments:**
         * *url* -- URL for the request.
         
        **Keyword arguments:**
         * *last_modified* -- Last modified date string to send as a request 
           header. (Default ``None``)
         * *etag* -- Etag string to send as a request header. (Default 
           ``None``)
         * *method* -- HTTP request method. (Default ``'GET'``)
         * *postdata* -- Dictionary of strings to post with the request. 
           (Default ``None``)
         * *headers* -- Dictionary of strings to send as request headers. 
           (Default ``None``)
         * *agent* -- User agent to send with request. (Default 
           ``'RequestQueuer'``)
         * *timeout* -- Request timeout, in seconds. (Default ``60``)
         * *cookies* -- Dictionary of strings to send as request cookies. 
           (Default ``None``).
         * *follow_redirect* -- Boolean switch to follow HTTP redirects. 
           (Default ``True``)
         * *prioritize* -- Move this request to the front of the request 
           queue. (Default ``False``)         

        """
        if headers is None:
            headers={}
        if postdata is not None:
            if isinstance(postdata, dict):
                for key in postdata:
                    postdata[key] = convertToUTF8(postdata[key])
                postdata = urllib.urlencode(postdata)
            else:
                convertToUTF8(postdata)
        if method.lower() == "post":
            headers["content-type"] = "application/x-www-form-urlencoded"
        if last_modified is not None:
            time_tuple = dateutil.parser.parse(last_modified).timetuple()
            time_string = time.strftime("%a, %d %b %Y %T %z", time_tuple)
            headers['If-Modified-Since'] = time_string
        if etag is not None:
            headers["If-None-Match"] = etag
        req = {
            "url":convertToUTF8(url),
            "method":method,
            "postdata":postdata,
            "headers":headers,
            "agent":agent,
            "timeout":timeout,
            "cookies":cookies,
            "follow_redirect":follow_redirect,
            "deferred":Deferred()
        }
        host = _parse(req["url"])[1]
        if host not in self.pending_reqs:
            self.pending_reqs[host] = []
        if prioritize:
            self.pending_reqs[host].insert(0, req)
        else:
            self.pending_reqs[host].append(req)
        self._checkActive()
        return req["deferred"]

    def _hostRequestCheck(self, host):
        if host not in self.pending_reqs:
            return False
        if host in self.last_req:
            if host in self.min_req_interval_per_hosts:
                if time.time() - self.last_req[host] < \
                    self.min_req_interval_per_hosts[host]:
                    return False
            else:
                if time.time() - self.last_req[host] < \
                    self.min_req_interval_per_host:
                    return False
        if host in self.active_reqs:
            if host in self.max_simul_reqs_per_hosts:
                if self.active_reqs[host] > self.max_simul_reqs_per_hosts[host]:
                    return False
            else:
                if self.active_reqs[host] > self.max_simul_reqs_per_host:
                    return False
        return True

    def _checkActive(self):
        
        #self.pending_reqs = dict([(x[0],x[1]) for x in filter(lambda x:len(x[1]) > 0, self.pending_reqs.items())])
        while self.getActive() < self.max_simul_reqs and self.getPending() > 0:        
            in_loop_req_count = 0
            hosts = self.pending_reqs.keys()
            for host in hosts:
                if self._hostRequestCheck(host):
                    if len(self.pending_reqs[host]) == 0:
                        del self.pending_reqs[host]
                        continue
                    else:
                        req = self.pending_reqs[host].pop(0)
                    in_loop_req_count += 1
                    d = self._getPage(req)
                    d.addCallback(self._requestComplete, req["deferred"], host)
                    d.addErrback(self._requestError, req["deferred"], host)
                    self.last_req[host] = time.time()
                    self.active_reqs[host] = self.active_reqs.get(host, 0) + 1
            if in_loop_req_count == 0:
                reactor.callLater(.1, self._checkActive)
                return
 

    def _requestComplete(self, response, deferred, host):
        self.active_reqs[host] -= 1
        self._checkActive()
        deferred.callback(response)
        return None

    def _requestError(self, error, deferred, host):
        self.active_reqs[host] -= 1
        self._checkActive()
        deferred.errback(error)
        return None

    def _getPage(self, req): 
        scheme, host, port = _parse(req['url'])[0:3]
        factory = HTTPClientFactory(
            req['url'],
            method=req['method'],
            postdata=req['postdata'],
            headers=req['headers'],
            agent=req['agent'],
            timeout=req['timeout'],
            cookies=req['cookies'],
            followRedirect=req['follow_redirect']
        )
        if scheme == 'https':
            context_factory = ssl.ClientContextFactory()
            reactor.connectSSL(
                                    host, 
                                    port, 
                                    factory, 
                                    context_factory, 
                                    timeout=req['timeout']
                                )
        else:
            reactor.connectTCP(host, port, factory, timeout=req['timeout'])
        factory.deferred.addCallback(self._getPageComplete, factory)
        factory.deferred.addErrback(self._getPageError, factory)
        return factory.deferred

    def _getPageComplete(self, response, factory):
        return {
                    "response":response, 
                    "headers":factory.response_headers, 
                    "status":int(factory.status), 
                    "message":factory.message
                }

    def _getPageError(self, error, factory):
        if hasattr(factory, "response_headers") \
            and factory.response_headers is not None:
            error.value.headers = factory.response_headers
        return error