#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Pagegetter."""

from hiispider.requestqueuer import RequestQueuer
import ujson
import marshal
from hashlib import sha1
from hiispider.exceptions import StaleContentException
from twisted.internet.defer import inlineCallbacks, returnValue


class PageGetter(object):

    def __init__(self, redis, rq=None):
        self.redis = redis
        if rq is None:
            self.rq = RequestQueuer()
        else:
            self.rq = rq
    
    @inlineCallbacks
    def getPage(self,
            url,
            method='GET',
            postdata=None,
            headers=None,
            agent="HiiSpider",
            timeout=5,
            cookies=None,
            follow_redirect=1,
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
        kwargs = {
            "method":method.upper(),
            "postdata":postdata,
            "headers":headers,
            "agent":agent,
            "timeout":timeout,
            "cookies":cookies,
            "follow_redirect":follow_redirect}
        url = url.encode("utf-8")
        cache = int(cache)
        if method != "GET" or cache == -1:
            data = yield self.rq.getPage(url, **kwargs)
        else:
            req_hash = hash(marshal.dumps([hash_url or url, agent, cookies]))
            if cache == 1:
                data = yield self._getCachedData(req_hash, url, kwargs)
            else:
                use_cache = self._checkCache(req_hash, url, kwargs)
                if use_cache:
                    data = yield self._getCachedData(req_hash, url, kwargs)
                else:
                    data = yield self.rq.getPage(url, **kwargs)
        if "content-sha1" not in data:
            data["content-sha1"] = sha1(data["response"]).hexdigest()
        if content_sha1 == data["content-sha1"]:
            logger.debug("Raising StaleContentException (4) on %s" % req_hash)
            raise StaleContentException(content_sha1)
        returnValue(data)
    
    def _checkCache(self, req_hash, url, kwargs):
        return False

