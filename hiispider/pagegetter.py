import simplejson
import cPickle as pickle
import datetime
import dateutil.parser
from hashlib import sha1
import logging
import time
import copy
from zlib import compress, decompress
from twisted.internet.defer import maybeDeferred, DeferredList
from .requestqueuer import RequestQueuer
from .unicodeconverter import convertToUTF8
from .exceptions import StaleContentException
from twisted.web.client import _parse
from twisted.python.failure import Failure

from hiispider import stats

class ReportedFailure(Failure):
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
logger = logging.getLogger(__name__)


class PageGetter:


    def __init__(self,
        cassandra_client,
        redis_client,
        disable_negative_cache=False,
        time_offset=0,
        rq=None):
        """
        Create an Cassandra based HTTP cache.

        **Arguments:**
            * *cassandra_client* -- Cassandra client object.
        **Keyword arguments:**
         * *rq* -- Request Queuer object. (Default ``None``)

        """
        self.cassandra_client = cassandra_client
        self.redis_client = redis_client
        self.disable_negative_cache = disable_negative_cache
        self.time_offset = time_offset
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq

    def _getPageCallback(self, url, request_hash, request_kwargs, cache, content_sha1, confirm_cache_write, host):
        if request_kwargs["method"] != "GET":
            d = self.rq.getPage(url, **request_kwargs)
            d.addCallback(self._checkForStaleContent, content_sha1, request_hash)
            return d
        if cache == -1:
            # Cache mode -1. Bypass cache entirely.
            logger.debug("Getting request %s for URL %s." % (request_hash, url))
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
            # Check if there is a cache entry, return headers.
            headers_key = 'headers:%s' % request_hash
            logger.debug("Checking redis headers key %s for URL %s." % (request_hash, url))
            d = self.redis_client.get(headers_key)
            d.addCallback(self._checkCacheHeaders,
                request_hash,
                url,
                request_kwargs,
                confirm_cache_write,
                content_sha1,
                host=host)
            d.addCallback(self._checkForStaleContent, content_sha1, request_hash)
            return d
        elif cache == 1:
            # Cache mode 1. Use cache immediately, if possible.
            logger.debug("Getting Cassandra object request %s for URL %s." % (request_hash, url))
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

    def _negativeReqCacheCallback(self, raw_negative_req_cache_item, negative_req_cache_key, url, request_hash, request_kwargs, cache, content_sha1, confirm_cache_write, host):
        if not self.disable_negative_cache:
            if raw_negative_req_cache_item:
                try:
                    negative_req_cache_item = pickle.loads(str(decompress(raw_negative_req_cache_item)))
                except Exception, e:
                    logger.critical(e)
                    negative_req_cache_item = None
                if negative_req_cache_item and negative_req_cache_item['timeout'] > time.time():
                    logger.error('Found request hash %s in negative request cache, raising last known exception' % request_hash)
                    stats.stats.increment('pg.negreqcache.hit')
                    negative_req_cache_item['error'].raiseException()
                else:
                    logger.error('Removing request hash %s from the negative request cache' % request_hash)
                    stats.stats.increment('pg.negreqcache.flush')
                    self.redis_client.delete(negative_req_cache_key)
        d = self._getPageCallback(url, request_hash, request_kwargs, cache, content_sha1, confirm_cache_write, host)
        return d

    def checkNegativeReqCache(self, data, negative_req_cache_key, url, request_hash, request_kwargs, cache, content_sha1, confirm_cache_write, host):
        d = self.redis_client.get(negative_req_cache_key)
        d.addCallback(self._negativeReqCacheCallback, negative_req_cache_key, url, request_hash, request_kwargs, cache, content_sha1, confirm_cache_write, host)
        return d

    def _negativeCacheCallback(self, raw_negative_cache_host, negative_cache_host_key, negative_req_cache_key, url, request_hash, request_kwargs, cache, content_sha1, confirm_cache_write, host):
        if not self.disable_negative_cache:
            if raw_negative_cache_host:
                try:
                    negative_cache_host = pickle.loads(str(decompress(raw_negative_cache_host)))
                except Exception, e:
                    logger.critical(e)
                    negative_cache_host = None
                if negative_cache_host and negative_cache_host['timeout'] > time.time():
                    logger.error('Found in negative cache, raising last known exception')
                    stats.stats.increment('pg.negcache.hit')
                    negative_cache_host['error'].raiseException()
                else:
                    logger.error('Removing host %s from the negative cache' % request_hash)
                    stats.stats.increment('pg.negcache.flush')
                    self.redis_client.delete(negative_cache_host_key)
        d = self.checkNegativeReqCache(None, negative_req_cache_key, url, request_hash, request_kwargs, cache, content_sha1, confirm_cache_write, host)
        return d

    def checkNegativeCache(self, negative_cache_host_key, negative_req_cache_key, url, request_hash, request_kwargs, cache, content_sha1, confirm_cache_write, host):
        d = self.redis_client.get(negative_cache_host_key)
        d.addCallback(self._negativeCacheCallback, negative_cache_host_key, negative_req_cache_key, url, request_hash, request_kwargs, cache, content_sha1, confirm_cache_write, host)
        return d

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
            check_only_tld=False,
            disable_negative_cache=False):
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
         * *check_only_tld* -- for negative cache, check only the top level domain name
         * *disable_negative_cache* -- disable negative cache for this request
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
        # check negative cache
        host = _parse(url)[1]
        # if check_only_tld is true then parse the url down to the top level domain
        if check_only_tld:
            host_split = host.split('.', host.count('.')-1)
            host = host_split[len(host_split)-1]
        # Create request_hash to serve as a cache key from
        # either the URL or user-provided hash_url.
        hash_items = [hash_url or url, agent]
        if postdata:
            hash_items.append(repr(postdata))
        if headers and 'Authorization' in headers:
            items = headers['Authorization'].split(',')
            oauth_headers = [item for item in items
                if item.find('oauth_consumer_key') > -1 or
                item.find('oauth_token') > -1 or
                item.find('oauth_token_secret') > -1]
            if oauth_headers:
                hash_items.append(repr(oauth_headers))
        if cookies:
            hash_items.append(repr(cookies))
        request_hash = sha1(simplejson.dumps(hash_items)).hexdigest()
        if not disable_negative_cache and not self.disable_negative_cache:
            negative_req_cache_key = 'negative_req_cache:%s' % request_hash
            negative_cache_host_key = 'negative_cache:%s' % host
            d = self.checkNegativeCache(
                    negative_cache_host_key,
                    negative_req_cache_key,
                    url,
                    request_hash,
                    request_kwargs,
                    cache,
                    content_sha1,
                    confirm_cache_write,
                    host,
                )
        else:
            d = self._getPageCallback(url, request_hash, request_kwargs, cache, content_sha1, confirm_cache_write, host)
        return d

    def getCachedData(self, request_hash):
        headers_key = 'headers:%s' % request_hash
        http_key = 'http:%s' % request_hash
        deferreds = []
        deferreds.append(self.redis_client.get(headers_key))
        deferreds.append(self.redis_client.get(http_key))
        d = DeferredList(deferreds, consumeErrors=True)
        d.addCallback(self._getCachedDataCallback)
        return d

    def _getCachedDataCallback(self, cached_data):
        response = simplejson.loads(decompress(cached_data[1][1]))
        if len(response) == 0:
            raise Exception("Empty cached data.")
        data = {
            "headers": simplejson.loads(decompress(cached_data[0][1])),
            "response": response,
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
        if not headers:
            d = self._requestWithNoCacheHeaders(
                None,
                request_hash,
                url,
                request_kwargs,
                confirm_cache_write,
                host=host,
            )
            return d
        else:
            logger.debug("Got redis object request %s for URL %s." % (request_hash, url))
            http_history = {}
            #if "content-length" in headers and int(headers["content-length"][0]) == 0:
            #    raise Exception("Zero Content length, do not use as cache.")
            if "content-sha1" in headers:
                if isinstance(headers["content-sha1"], (list, tuple)):
                    http_history["content-sha1"] = headers["content-sha1"][0]
                else:
                    http_history["content-sha1"] = headers["content-sha1"]
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
                if isinstance(headers["cache-expires"], (list, tuple)):
                    headers["cache-expires"] = headers["cache-expires"][0]
                expires = time.mktime(dateutil.parser.parse(headers["cache-expires"]).timetuple())
                now = time.mktime(datetime.datetime.now(UTC).timetuple())
                if expires > now:
                    if "content-sha1" in http_history and http_history["content-sha1"] == content_sha1:
                        logger.debug("Raising StaleContentException (1) on %s" % request_hash)
                        stats.stats.increment('pg.stalecontent')
                        raise StaleContentException()
                    logger.debug("Cached data %s for URL %s is not stale. Getting from redis." % (request_hash, url))
                    stats.stats.increment('pg.redis.hit')
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
            stats.stats.increment('pg.redis.miss')
            modified_request_kwargs = copy.deepcopy(request_kwargs)
            # At this point, cached data may or may not be stale.
            # If cached data has an etag header, include it in the request.
            if "cache-etag" in headers:
                modified_request_kwargs["etag"] = headers["cache-etag"]
            # If cached data has a last-modified header, include it in the request.
            if "cache-last-modified" in headers:
                modified_request_kwargs["last_modified"] = headers["cache-last-modified"]
            logger.debug("Requesting %s for URL %s with etag and last-modified headers." % (request_hash, url))
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
        logger.debug("Got request %s for URL %s." % (request_hash, url))
        data["pagegetter-cache-hit"] = False
        data["content-sha1"] = sha1(data["response"]).hexdigest()
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
            logger.debug("Raising StaleContentException (2) on %s\nError: %s" % (request_hash, str(e)))
            raise StaleContentException()
        except Exception:
            pass
        # No header stored in the cache. Make the request.
        logger.debug("Unable to find header for request %s on redis, fetching from %s." % (request_hash, url))
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

    def _negativeCacheWriteCallback(self, data, key):
        d = self.redis_client.expire(key, 60*60*24)
        d.addCallback(self._negativeCacheWriteCallback2)
        return d

    def _negativeCacheWriteCallback2(self, data):
        return

    def _negativeCacheWriteErrback(self, error):
        logger.error('Error writing to negative cache: %s' % str(error))
        return

    def _setNegativeReqCacheCallback(self, data, error, negative_req_cache_key):
        if data:
            negative_req_cache_item = pickle.loads(str(decompress(data)))
            if negative_req_cache_item['retries'] <= 5:
                negative_req_cache_item['timeout'] = time.time() + 3600
                negative_req_cache_item['retries'] += 1
            else:
                negative_req_cache_item['timeout'] = time.time() + 10800
                negative_req_cache_item['retries'] += 1
        else:
            negative_req_cache_item = {
                'timeout': time.time() + 600,
                'retries': 1,
            }
        negative_req_cache_item['error'] = error
        logger.error('Updating negative request cache %s which has failed %d times' % (negative_req_cache_key, negative_req_cache_item['retries']))
        negative_req_cache_item_pickle = compress(pickle.dumps(negative_req_cache_item), 1)
        d = self.redis_client.set(negative_req_cache_key, negative_req_cache_item_pickle)
        d.addCallback(self._negativeCacheWriteCallback, negative_req_cache_key)
        d.addErrback(self._negativeCacheWriteErrback)
        return d

    def setNegativeReqCache(self, error, request_hash):
        negative_req_cache_key = 'negative_req_cache:%s' % request_hash
        d = self.redis_client.get(negative_req_cache_key)
        d.addCallback(self._setNegativeReqCacheCallback, error, negative_req_cache_key)
        return d

    def _setNegativeCacheCallback(self, data, error, host, negative_cache_key):
        if data:
            negative_cache_item = pickle.loads(str(decompress(data)))
            if negative_cache_item['retries'] <= 10:
                negative_cache_item['timeout'] = time.time() + 300
                negative_cache_item['retries'] += 1
            if negative_cache_item['retries'] <= 50:
                negative_cache_item['timeout'] = time.time() + 600
                negative_cache_item['retries'] += 1
            else:
                negative_cache_item['timeout'] = time.time() + 3600
                negative_cache_item['retries'] += 1
        else:
            negative_cache_item = {
                'timeout': time.time() + 300,
                'retries': 1,
            }
        negative_cache_item['error'] = error
        logger.error('Updating negative cache for host %s which has failed %d times' % (host, negative_cache_item['retries']))
        negative_cache_item_pickle = compress(pickle.dumps(negative_cache_item), 1)
        d = self.redis_client.set(negative_cache_key, negative_cache_item_pickle)
        d.addCallback(self._negativeCacheWriteCallback, negative_cache_key)
        d.addErrback(self._negativeCacheWriteErrback)
        return d

    def setNegativeCache(self, error, host):
        negative_cache_key = 'negative_cache:%s' % host
        d = self.redis_client.get(negative_cache_key)
        d.addCallback(self._setNegativeCacheCallback, error, host, negative_cache_key)
        return d

    def _requestWithNoCacheHeadersErrback(self,
            error,
            request_hash,
            url,
            confirm_cache_write,
            request_kwargs,
            http_history=None,
            host=None):
        logger.error(error.value.__dict__)
        logger.error("Unable to get request %s for URL %s.\n%s" % (
            request_hash,
            url,
            error))
        try:
            status = int(error.value.status)
        except:
            # FIXME non server responses should be negative cached, but
            # twitter is having issues and thus we are not updating peoples
            # profiles
            if 'twitter' in url:
                return error
            status = 500
        if 'tumblr.com' in url and status == 400:
            status = 500
        elif 'twitter.com' in url and status == 502:
            # FIXME twitter says a 502 is a down for maint, but we are getting it when clearly
            # they are not and is messing up negative cache
            return error
        if status >= 400 and status < 500:
            self.setNegativeReqCache(error, request_hash)
        if status >= 500:
            self.setNegativeCache(error, host)
        if http_history is None:
            http_history = {}
        if "request-failures" not in http_history:
            http_history["request-failures"] = [str(int(self.time_offset + time.time()))]
        else:
            http_history["request-failures"].append(str(int(self.time_offset + time.time())))
        http_history["request-failures"] = http_history["request-failures"][-3:]
        logger.debug("Writing data for failed request %s to redis." % request_hash)
        headers = {}
        headers["request-failures"] = ",".join(http_history["request-failures"])
        headers_key = 'headers:%s' % request_hash
        http_key = 'http:%s' % request_hash
        deferreds = []
        deferreds.append(self.redis_client.set(headers_key, compress(simplejson.dumps(headers), 1)))
        deferreds.append(self.redis_client.delete(http_key, compress(simplejson.dumps(""), 1)))
        d = DeferredList(deferreds, consumeErrors=True)
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
        try:
            status = int(error.value.status)
        except:
            # FIXME non server responses should be negative cached, but
            # twitter is having issues and thus we are not updating peoples
            # profiles
            if 'twitter.com' in url:
                return ReportedFailure(error)
            status = 500
        if 'twitter.com' in url and status == 502:
            # FIXME twitter says a 502 is a down for maint, but we are getting it when clearly
            # they are not and is messing up negative cache
            return ReportedFailure(error)
        if status == 304:
            if "content-sha1" in http_history and http_history["content-sha1"] == content_sha1:
                logger.debug("Raising StaleContentException (3) on %s" % request_hash)
                raise StaleContentException()
            logger.debug("Request %s for URL %s hasn't been modified since it was last downloaded. Getting data from Cassandra." % (request_hash, url))
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
            logger.debug("Writing data for failed request %s to redis. %s" % (request_hash, error))
            headers = {}
            headers["request-failures"] = ",".join(http_history["request-failures"])
            headers_key = 'headers:%s' % request_hash
            d = self.redis_client.set(headers_key, compress(simplejson.dumps(headers), 1))
            if confirm_cache_write:
                d.addCallback(self._handleRequestWithCacheHeadersErrorCallback, error)
                return d
            return ReportedFailure(error)

    def _handleRequestWithCacheHeadersErrorCallback(self, data, error):
        return ReportedFailure(error)

    def _returnCachedData(self, data, request_hash):
        logger.debug("Got request %s from Cassandra." % (request_hash))
        data["pagegetter-cache-hit"] = True
        data["status"] = 304
        data["message"] = "Not Modified"
        if "content-sha1" in data["headers"]:
            data["content-sha1"] = data["headers"]["content-sha1"]
            del data["headers"]["content-sha1"]
        else:
            data["content-sha1"] = sha1(data["response"]).hexdigest()
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
        #data["content-sha1"] = sha1(data["response"]).hexdigest()
        if http_history is None:
            http_history = {}
        if "content-sha1" not in http_history:
            http_history["content-sha1"] = data["content-sha1"]
        if "content-changes" not in http_history:
            http_history["content-changes"] = []
        if data["content-sha1"] != http_history["content-sha1"]:
            http_history["content-changes"].append(str(int(self.time_offset + time.time())))
        http_history["content-changes"] = http_history["content-changes"][-10:]
        headers = {}
        http_history["content-changes"] = filter(lambda x:len(x) > 0, http_history["content-changes"])
        headers["content-changes"] = ",".join(http_history["content-changes"])
        headers["content-sha1"] = data["content-sha1"]
        if "cache-control" in data["headers"]:
            if isinstance(data["headers"]["cache-control"], (list, tuple)):
                if "no-cache" in data["headers"]["cache-control"][0]:
                    return data
            else:
                if "no-cache" in data["headers"]["cache-control"]:
                    return data
        if "expires" in data["headers"]:
            if isinstance(data["headers"]["expires"], (list, tuple)):
                headers["cache-expires"] = data["headers"]["expires"][0]
            else:
                headers["cache-expires"] = data["headers"]["expires"]
        if "etag" in data["headers"]:
            if isinstance(data["headers"]["etag"], (list, tuple)):
                headers["cache-etag"] = data["headers"]["etag"][0]
            else:
                headers["cache-etag"] = data["headers"]["etag"]
        if "last-modified" in data["headers"]:
            if isinstance(data["headers"]["last-modified"], (list, tuple)):
                headers["cache-last-modified"] = data["headers"]["last-modified"][0]
            else:
                headers["cache-last-modified"] = data["headers"]["last-modified"]
        if "content-type" in data["headers"]:
            if isinstance(data["headers"]["content-type"], (list, tuple)):
                headers["content_type"] = data["headers"]["content-type"][0]
            else:
                headers["content_type"] = data["headers"]["content-type"]
        headers_key = 'headers:%s' % request_hash
        http_key = 'http:%s' % request_hash
        logger.debug("Writing data for request %s to redis." % request_hash)
        deferreds = []
        deferreds.append(self.redis_client.set(headers_key, compress(simplejson.dumps(headers), 1)))
        deferreds.append(self.redis_client.set(http_key, compress(simplejson.dumps(data["response"]), 1)))
        d = DeferredList(deferreds, consumeErrors=True)
        if confirm_cache_write:
            d.addCallback(self._storeDataCallback, data)
            d.addErrback(self._storeDataErrback, data, request_hash)
            return d
        return data

    def _storeDataCallback(self, data, response_data):
        return response_data

    def _storeDataErrback(self, error, response_data, request_hash):
        logger.error("Error storing data for %s" % (request_hash))
        return response_data

    def _checkForStaleContent(self, data, content_sha1, request_hash):
        if "content-sha1" not in data:
            data["content-sha1"] = sha1(data["response"]).hexdigest()
        if content_sha1 == data["content-sha1"]:
            logger.debug("Raising StaleContentException (4) on %s" % request_hash)
            raise StaleContentException(content_sha1)
        else:
            return data
