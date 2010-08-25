import cjson
import twisted.python.failure
import datetime
import dateutil.parser
import hashlib
import logging
import time
import copy
from twisted.internet.defer import maybeDeferred
from .requestqueuer import RequestQueuer
from .unicodeconverter import convertToUTF8, convertToUnicode
from .exceptions import StaleContentException

class ReportedFailure(twisted.python.failure.Failure):
    pass

# A UTC class.
class CoordinatedUniversalTime(datetime.tzinfo):
    
    ZERO = datetime.timedelta(0)
    
    def utcoffset(self, dt):
        return self.ZERO
        
    def tzname(self, dt):
        return "UTC"
        
    def dst(self, dt):
        return self.ZERO


UTC = CoordinatedUniversalTime()
LOGGER = logging.getLogger("main")


class PageGetter:
    
    def __init__(self, rq=None):
        """
        Create an Cassandra based HTTP cache.

        **Arguments:**
         * *cassandra_client* -- Cassandra client object.
         * *cassandra_cf_cache -- Cassandra CF to use for the HTTP cache.

        **Keyword arguments:**
         * *rq* -- Request Queuer object. (Default ``None``)      

        """
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq
    
        
    def getPage(self, 
            url, 
            method='GET', 
            postdata=None,
            headers=None, 
            agent="HiiSpider", 
            timeout=60, 
            cookies=None, 
            follow_redirect=1, 
            prioritize=False,
            hash_url=None, 
            cache=0,
            content_sha1=None,
            confirm_cache_write=False):
        """
        Make a cached HTTP Request.

        **Arguments:**
         * *url* -- URL for the request.

        **Keyword arguments:**
         * *method* -- HTTP request method. (Default ``'GET'``)
         * *postdata* -- Dictionary of strings to post with the request. 
           (Default ``None``)
         * *headers* -- Dictionary of strings to send as request headers. 
           (Default ``None``)
         * *agent* -- User agent to send with request. (Default 
           ``'HiiSpider'``)
         * *timeout* -- Request timeout, in seconds. (Default ``60``)
         * *cookies* -- Dictionary of strings to send as request cookies. 
           (Default ``None``).
         * *follow_redirect* -- Boolean switch to follow HTTP redirects. 
           (Default ``True``)
         * *prioritize* -- Move this request to the front of the request 
           queue. (Default ``False``)
         * *hash_url* -- URL string used to indicate a common resource.
           Example: "http://digg.com" and "http://www.digg.com" could both
           use hash_url, "http://digg.com" (Default ``None``)      
         * *cache* -- Cache mode. ``1``, immediately return contents of 
           cache if available. ``0``, check resource, return cache if not 
           stale. ``-1``, ignore cache. (Default ``0``)
         * *content_sha1* -- SHA-1 hash of content. If this matches the 
           hash of data returned by the resource, raises a 
           StaleContentException.  
         * *confirm_cache_write* -- Wait to confirm cache write before returning.       
        """ 
        request_kwargs = {
            "method":method.upper(), 
            "postdata":postdata, 
            "headers":headers, 
            "agent":agent, 
            "timeout":timeout, 
            "cookies":cookies, 
            "follow_redirect":follow_redirect, 
            "prioritize":prioritize}
        cache = int(cache)
        cache=0
        if cache not in [-1,0,1]:
            raise Exception("Unknown caching mode.")
        if not isinstance(url, str):
            url = convertToUTF8(url)
        if hash_url is not None and not isinstance(hash_url, str):
            hash_url = convertToUTF8(hash_url)
        # Create request_hash to serve as a cache key from
        # either the URL or user-provided hash_url.
        if hash_url is None:
            request_hash = hashlib.sha1(cjson.encode([
                url, 
                agent])).hexdigest()
        else:
            request_hash = hashlib.sha1(cjson.encode([
                hash_url, 
                agent])).hexdigest()

        d = self.rq.getPage(url, **request_kwargs)
        d.addCallback(self._checkForStaleContent, content_sha1, request_hash)
        return d

    def _checkForStaleContent(self, data, content_sha1, request_hash):
        if "content-sha1" not in data:
            data["content-sha1"] = hashlib.sha1(data["response"]).hexdigest()
        if content_sha1 == data["content-sha1"]:
            LOGGER.debug("Raising StaleContentException (4) on %s" % request_hash)
            raise StaleContentException(content_sha1)
        else:
            return data