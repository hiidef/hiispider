#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Pagegetter."""

import sys
import ujson as json
import cPickle as pickle
import datetime
import dateutil.parser
from hashlib import sha1
import logging
import time
import copy
from zlib import compress, decompress
from twisted.internet.defer import maybeDeferred, DeferredList, inlineCallbacks, returnValue
from twisted.internet.error import ConnectionDone

from hiispider.requestqueuer import RequestQueuer
from hiispider.unicodeconverter import convertToUTF8

from twisted.web.client import _parse
from twisted.python.failure import Failure

from hiispider.exceptions import StaleContentException, NegativeHostCacheException,\
    NegativeReqCacheException

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


class PageGetter(object):

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
        stats.stats.increment("pg.getpage", 0.05)
        start = time.time()
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
        request_hash = sha1(json.dumps(hash_items)).hexdigest()
        data = yield self.rq.getPage(url, **request_kwargs)
        if "content-sha1" not in data:
            data["content-sha1"] = sha1(data["response"]).hexdigest()
        if content_sha1 == data["content-sha1"]:
            stats.stats.increment('pg.stalecontent')
            raise StaleContentException(content_sha1)
        returnValue(data)

