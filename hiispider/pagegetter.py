import cjson
import twisted.python.failure
import datetime
import dateutil.parser
import hashlib
import logging
import time
import copy
from twisted.internet.defer import maybeDeferred, DeferredList
from .requestqueuer import RequestQueuer
from .unicodeconverter import convertToUTF8, convertToUnicode
from .exceptions import StaleContentException
import zlib
from twisted.web.client import _parse


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
    
    negitive_cache = {}
    negitive_req_cache = {}
    
    def __init__(self, 
        cassandra_client, 
        cassandra_cf_cache,
        cassandra_http,
        cassandra_headers,
        time_offset=0,
        rq=None):
        """
        Create an Cassandra based HTTP cache.

        **Arguments:**
            * *cassandra_client* -- Cassandra client object.
            * *cassandra_cf_cache -- Cassandra CF to use for the HTTP cache.
        **Keyword arguments:**
         * *rq* -- Request Queuer object. (Default ``None``)      

        """
        self.cassandra_client = cassandra_client
        self.cassandra_cf_cache = cassandra_cf_cache
        self.cassandra_http = cassandra_http
        self.cassandra_headers = cassandra_headers
        self.time_offset = time_offset
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq
    
    # def clearCache(self):
    #     """
    #     Clear the S3 bucket containing the S3 cache.
    #     """
    #     d = self.s3.emptyBucket(self.aws_s3_http_cache_bucket)
    #     return d
        
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
            confirm_cache_write=False,
            check_only_tld=False):
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
         * *check_only_tld* -- for negitive cache, check only the top level domain name
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
        if cache not in [-1,0,1]:
            raise Exception("Unknown caching mode.")
        if not isinstance(url, str):
            url = convertToUTF8(url)
        if hash_url is not None and not isinstance(hash_url, str):
            hash_url = convertToUTF8(hash_url)
        # check negitive cache
        host = _parse(url)[1]
        # if check_only_tld is true then parse the url down to the top level domain
        if check_only_tld:
            host_split = host.split('.', host.count('.')-1)
            host = host_split[len(host_split)-1]
        if host in self.negitive_cache:
            if self.negitive_cache[host]['timeout'] > time.time():
                LOGGER.error('Found %s in negitive cache, raising last known exception' % host)
                return self.negitive_cache[host]['error'].raiseException()
            else:
                try:
                    del self.negitive_cache[host]
                except:
                    pass
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
        # check to see if the request hash is in the negitive_req_cache
        # if request_hash in self.negitive_req_cache:
        #     if self.negitive_req_cache[request_hash]['timeout'] > time.time():
        #         LOGGER.error('Found request hash %s in negitive request cache, raising last known exception' % request_hash)
        #         return self.negitive_req_cache[hash_url]['error'].raiseException()
        #     else:
        #         try:
        #             LOGGER.error('Removing request hash %s from the negitive request cache' % request_hash)
        #             del self.negitive_req_cache[request_hash]
        #         except:
        #             pass
        if request_kwargs["method"] != "GET":
            d = self.rq.getPage(url, **request_kwargs)
            d.addCallback(self._checkForStaleContent, content_sha1, request_hash)
            return d
        if cache == -1:
            # Cache mode -1. Bypass cache entirely.
            LOGGER.debug("Getting request %s for URL %s." % (request_hash, url))
            d = self.rq.getPage(url, **request_kwargs)
            d.addCallback(self._returnFreshData, 
                request_hash, 
                url,  
                confirm_cache_write)
            d.addErrback(self._requestWithNoCacheHeadersErrback,
                request_hash, 
                url, 
                confirm_cache_write,
                request_kwargs,
                host=host)
            d.addCallback(self._checkForStaleContent, content_sha1, request_hash)
            return d
        elif cache == 0:
            # Cache mode 0. Check cache, send cached headers, possibly use cached data.
            LOGGER.debug("Checking Cassandra head object request %s for URL %s." % (request_hash, url))
            # Check if there is a cache entry, return headers.
            d = self.cassandra_client.get(
                request_hash,
                self.cassandra_cf_cache,
                column=self.cassandra_headers)
            d.addCallback(self._checkCacheHeaders, 
                request_hash,
                url,  
                request_kwargs,
                confirm_cache_write,
                content_sha1,
                host=host)
            d.addErrback(self._requestWithNoCacheHeaders, 
                request_hash, 
                url, 
                request_kwargs,
                confirm_cache_write,
                host=host)
            d.addCallback(self._checkForStaleContent, content_sha1, request_hash)    
            return d
        elif cache == 1:
            # Cache mode 1. Use cache immediately, if possible.
            LOGGER.debug("Getting Cassandra object request %s for URL %s." % (request_hash, url))
            d = self.getCachedData(request_hash)
            d.addCallback(self._returnCachedData, request_hash)
            d.addErrback(self._requestWithNoCacheHeaders, 
                request_hash, 
                url, 
                request_kwargs,
                confirm_cache_write,
                host=host)
            d.addCallback(self._checkForStaleContent, content_sha1, request_hash)    
            return d      
    
    def getCachedData(self, request_hash):
        d = self.cassandra_client.get_slice(
                request_hash,
                self.cassandra_cf_cache,
                names=(self.cassandra_headers, self.cassandra_http))
        d.addCallback(self._getCachedDataCallback)
        return d
    
    def _getCachedDataCallback(self, cached_data):
        response = cjson.decode(zlib.decompress(cached_data[1].column.value))
        if len(response) == 0:
            raise Exception("Empty cached data.")
        data = {
            "headers": cjson.decode(zlib.decompress(cached_data[0].column.value)),
            "response": response
        }
        return data
    
    def _checkCacheHeaders(self, 
            headers, 
            request_hash, 
            url, 
            request_kwargs,
            confirm_cache_write,
            content_sha1,
            host=None):
        headers = cjson.decode(zlib.decompress(headers.column.value))
        LOGGER.debug("Got Cassandra object request %s for URL %s." % (request_hash, url))
        http_history = {}
        #if "content-length" in headers and int(headers["content-length"][0]) == 0:
        #    raise Exception("Zero Content length, do not use as cache.")
        if "content-sha1" in headers:
            http_history["content-sha1"] = headers["content-sha1"][0]
        # Filter?
        if "request-failures" in headers:
            request_failures = headers["request-failures"].split(",")
            if len(request_failures) > 0:
                http_history["request-failures"] = request_failures
        if "content-changes" in headers:
            content_changes = headers["content-changes"].split(",")
            if len(content_changes) > 0:
                http_history["content-changes"] = content_changes
        # If cached data is not stale, return it.
        if "cache-expires" in headers:
            expires = dateutil.parser.parse(headers["cache-expires"][0])
            now = datetime.datetime.now(UTC)
            if expires > now:
                if "content-sha1" in http_history and http_history["content-sha1"] == content_sha1:
                    LOGGER.debug("Raising StaleContentException (1) on %s" % request_hash)
                    raise StaleContentException()
                LOGGER.debug("Cached data %s for URL %s is not stale. Getting from Cassandra." % (request_hash, url))
                d = self.getCachedData(request_hash)
                d.addCallback(self._returnCachedData, request_hash)
                d.addErrback(
                    self._requestWithNoCacheHeaders, 
                    request_hash, 
                    url,
                    request_kwargs, 
                    confirm_cache_write,
                    http_history=http_history,
                    host=host)
                return d
        modified_request_kwargs = copy.deepcopy(request_kwargs)
        # At this point, cached data may or may not be stale.
        # If cached data has an etag header, include it in the request.
        if "cache-etag" in headers:
            modified_request_kwargs["etag"] = headers["cache-etag"][0]
        # If cached data has a last-modified header, include it in the request.
        if "cache-last-modified" in headers:
            modified_request_kwargs["last_modified"] = headers["cache-last-modified"][0]
        LOGGER.debug("Requesting %s for URL %s with etag and last-modified headers." % (request_hash, url))
        # Make the request. A callback means a 20x response. An errback 
        # could be a 30x response, indicating the cache is not stale.
        d = self.rq.getPage(url, **modified_request_kwargs)
        d.addCallback(
            self._returnFreshData, 
            request_hash,
            url, 
            confirm_cache_write,
            http_history=http_history)
        d.addErrback(
            self._handleRequestWithCacheHeadersError, 
            request_hash, 
            url, 
            request_kwargs, 
            confirm_cache_write,
            headers,
            http_history,
            content_sha1)
        return d
        
    def _returnFreshData(self, 
            data, 
            request_hash, 
            url,  
            confirm_cache_write,
            http_history=None):
        LOGGER.debug("Got request %s for URL %s." % (request_hash, url))
        data["pagegetter-cache-hit"] = False
        data["content-sha1"] = hashlib.sha1(data["response"]).hexdigest()
        if http_history is not None and "content-sha1" in http_history:
            if http_history["content-sha1"] == data["content-sha1"]:
                return data
        d = maybeDeferred(self._storeData,
            data, 
            request_hash,  
            confirm_cache_write,
            http_history=http_history)
        d.addErrback(self._storeDataErrback, data, request_hash)
        return d

    def _requestWithNoCacheHeaders(self, 
            error, 
            request_hash, 
            url, 
            request_kwargs, 
            confirm_cache_write,
            http_history=None,
            host=None):
        try:
            error.raiseException()
        except StaleContentException, e:
            LOGGER.debug("Raising StaleContentException (2) on %s" % request_hash)
            raise StaleContentException()
        except Exception, e:
            pass
        # No header stored in the cache. Make the request.
        LOGGER.debug("Unable to find header for request %s on Cassandra, fetching from %s." % (request_hash, url))
        d = self.rq.getPage(url, **request_kwargs)
        d.addCallback(
            self._returnFreshData, 
            request_hash, 
            url, 
            confirm_cache_write,
            http_history=http_history)
        d.addErrback(
            self._requestWithNoCacheHeadersErrback, 
            request_hash, 
            url, 
            confirm_cache_write,
            request_kwargs,
            http_history=http_history,
            host=host)
        return d        
    
    def _requestWithNoCacheHeadersErrback(self, 
            error,     
            request_hash, 
            url, 
            confirm_cache_write,
            request_kwargs,
            http_history=None,
            host=None):
        LOGGER.error(error.value.__dict__)
        LOGGER.error("Unable to get request %s for URL %s.\n%s" % (
            request_hash, 
            url, 
            error))
        try:
            status = int(error.value.status)
        except:
            status = 500
        # if status >= 400 and status < 500:
        #     if not request_hash in self.negitive_req_cache:
        #         LOGGER.error('Adding request hash %s to negitive request cache' % request_hash)
        #         self.negitive_req_cache[request_hash] = {
        #             'timeout': time.time() + 600,
        #             'retries': 1,
        #             'error': error
        #         }
        #     else:
        #         if self.negitive_req_cache[request_hash]['retries'] <= 5:
        #             self.negitive_req_cache[request_hash]['timeout'] = time.time() + 1200
        #             self.negitive_req_cache[request_hash]['retries'] += 1
        #         else:
        #             self.negitive_req_cache[request_hash]['timeout'] = time.time() + 7200
        #             self.negitive_req_cache[request_hash]['retries'] += 1
        #         self.negitive_req_cache[request_hash]['error'] = error
        #         LOGGER.error('Updating negitive request cache for requset hash %s which has failed %d times' % (host, self.negitive_req_cache[request_hash]['retries']))
        if status >= 500:
            if not host in self.negitive_cache:
                LOGGER.error('Adding %s to negitive cache' % host)
                self.negitive_cache[host] = {
                    'timeout': time.time() + 300,
                    'retries': 1,
                    'error': error
                }
            else:
                if self.negitive_cache[host]['retries'] <= 5:
                    self.negitive_cache[host]['timeout'] = time.time() + 600
                    self.negitive_cache[host]['retries'] += 1
                else:
                    self.negitive_cache[host]['timeout'] = time.time() + 3600
                    self.negitive_cache[host]['retries'] += 1
                self.negitive_cache[host]['error'] = error
                LOGGER.error('Updating negitive cache for host %s which has failed %d times' % (host, self.negitive_cache[host]['retries']))
        if http_history is None:
            http_history = {}
        if "request-failures" not in http_history:
            http_history["request-failures"] = [str(int(self.time_offset + time.time()))]
        else:
            http_history["request-failures"].append(str(int(self.time_offset + time.time())))
        http_history["request-failures"] = http_history["request-failures"][-3:]
        LOGGER.debug("Writing data for failed request %s to Cassandra." % request_hash)
        headers = {}
        headers["request-failures"] = ",".join(http_history["request-failures"])
        d = self.cassandra_client.batch_insert(
            request_hash,
            self.cassandra_cf_cache,
            {
                self.cassandra_http: zlib.compress(cjson.encode(""), 1),
                self.cassandra_headers: zlib.compress(cjson.encode(headers), 1),
            },
        )
        if confirm_cache_write:
            d.addCallback(self._requestWithNoCacheHeadersErrbackCallback, error)
            return d       
        return error
        
    def _requestWithNoCacheHeadersErrbackCallback(self, data, error):
        return error
    
    def _handleRequestWithCacheHeadersError(self, 
            error, 
            request_hash, 
            url, 
            request_kwargs,  
            confirm_cache_write,
            previous_headers,
            http_history,
            content_sha1):
        if error.value.status == "304":
            if "content-sha1" in http_history and http_history["content-sha1"] == content_sha1:
                LOGGER.debug("Raising StaleContentException (3) on %s" % request_hash)
                raise StaleContentException()
            LOGGER.debug("Request %s for URL %s hasn't been modified since it was last downloaded. Getting data from Cassandra." % (request_hash, url))
            d = self.getCachedData(request_hash)
            d.addCallback(self._returnCachedData, request_hash)
            d.addErrback(
                self._requestWithNoCacheHeaders, 
                request_hash, 
                url, 
                request_kwargs, 
                confirm_cache_write,
                http_history=http_history)
            return d
        else:
            if http_history is None:
                http_history = {} 
            if "request-failures" not in http_history:
                http_history["request-failures"] = [str(int(self.time_offset + time.time()))]
            else:
                http_history["request-failures"].append(str(int(self.time_offset + time.time())))
            http_history["request-failures"] = http_history["request-failures"][-3:]
            LOGGER.debug("Writing data for failed request %s to Cassandra. %s" % (request_hash, error))
            headers = {}
            headers["request-failures"] = ",".join(http_history["request-failures"])
            encoded_data = zlib.compress(cjson.encode(headers), 1)
            d = self.cassandra_client.insert(
                request_hash, 
                self.cassandra_cf_cache,
                encoded_data,
                column=self.cassandra_headers)
            if confirm_cache_write:
                d.addCallback(self._handleRequestWithCacheHeadersErrorCallback, error)
                return d
            return ReportedFailure(error)
            
    def _handleRequestWithCacheHeadersErrorCallback(self, data, error):
        return ReportedFailure(error)
        
    def _returnCachedData(self, data, request_hash):
        LOGGER.debug("Got request %s from Cassandra." % (request_hash))
        data["pagegetter-cache-hit"] = True
        data["status"] = 304
        data["message"] = "Not Modified"
        if "content-sha1" in data["headers"]:
            data["content-sha1"] = data["headers"]["content-sha1"][0]
            del data["headers"]["content-sha1"]
        else:
            data["content-sha1"] = hashlib.sha1(data["response"]).hexdigest()
        if "cache-expires" in data["headers"]:
            data["headers"]["expires"] = data["headers"]["cache-expires"]
            del data["headers"]["cache-expires"]
        if "cache-etag" in data["headers"]:
            data["headers"]["etag"] = data["headers"]["cache-etag"]
            del data["headers"]["cache-etag"]
        if "cache-last-modified" in data["headers"]:
            data["headers"]["last-modified"] = data["headers"]["cache-last-modified"]
            del data["headers"]["cache-last-modified"]
        return data
            
    def _storeData(self, 
            data, 
            request_hash,  
            confirm_cache_write,
            http_history=None):
        if len(data["response"]) == 0:
            return self._storeDataErrback(Failure(exc_value=Exception("Response data is of length 0")), data, request_hash)
        #data["content-sha1"] = hashlib.sha1(data["response"]).hexdigest()
        if http_history is None:
            http_history = {} 
        if "content-sha1" not in http_history:
            http_history["content-sha1"] = data["content-sha1"]
        if "content-changes" not in http_history:
            http_history["content-changes"] = []
        if data["content-sha1"] != http_history["content-sha1"]:
            http_history["content-changes"].append(str(int(self.time_offset + time.time())))
        http_history["content-changes"] = http_history["content-changes"][-10:]
        LOGGER.debug("Writing data for request %s to Cassandra." % request_hash)
        headers = {}
        http_history["content-changes"] = filter(lambda x:len(x) > 0, http_history["content-changes"])
        headers["content-changes"] = ",".join(http_history["content-changes"])
        headers["content-sha1"] = data["content-sha1"]
        if "cache-control" in data["headers"]: 
            if "no-cache" in data["headers"]["cache-control"][0]:
                return data
        if "expires" in data["headers"]:
            headers["cache-expires"] = data["headers"]["expires"][0]
        if "etag" in data["headers"]:
            headers["cache-etag"] = data["headers"]["etag"][0]
        if "last-modified" in data["headers"]:
            headers["cache-last-modified"] = data["headers"]["last-modified"][0]
        if "content-type" in data["headers"]:
            content_type = data["headers"]["content-type"][0]
        d = self.cassandra_client.batch_insert(
            request_hash, 
            self.cassandra_cf_cache, 
            {
                self.cassandra_http: zlib.compress(cjson.encode(data["response"]), 1),
                self.cassandra_headers: zlib.compress(cjson.encode(headers), 1),
            },
        )
        if confirm_cache_write:
            d.addCallback(self._storeDataCallback, data)
            d.addErrback(self._storeDataErrback, data, request_hash)
            return d
        return data
        
    def _storeDataCallback(self, data, response_data):
        return response_data
    
    def _storeDataErrback(self, error, response_data, request_hash):
        LOGGER.error("Error storing data for %s" % (request_hash))
        return response_data

    def _checkForStaleContent(self, data, content_sha1, request_hash):
        if "content-sha1" not in data:
            data["content-sha1"] = hashlib.sha1(data["response"]).hexdigest()
        if content_sha1 == data["content-sha1"]:
            LOGGER.debug("Raising StaleContentException (4) on %s" % request_hash)
            raise StaleContentException(content_sha1)
        else:
            return data
