#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Base resources."""

from twisted.python.failure import Failure
from twisted.web.resource import Resource
import cStringIO, gzip
import traceback
import ujson as json
import logging
import pprint

logger = logging.getLogger(__name__)

class BaseResource(Resource):

    def __init__(self):
        Resource.__init__(self)

    def _successResponse(self, data):
        # if isinstance(data, str):
        #      return data
        return json.dumps(data)

    def _errorResponse(self, error):
        reason = str(error.value)
        # there are two ways to extract a traceback;  we pick whichever one
        # is longer as that probably has more information
        tbs = [error.getTraceback(),
               traceback.format_exc(traceback.extract_tb(error.tb))]
        tbs.sort(key=lambda x: len(x), reverse=True)
        tb = tbs[0]
        logger.error("%s\n%s" % (reason, tb))
        return json.dumps({"error":reason, "traceback":tb})

    def _immediateResponse(self, data, request):
        # logger.debug("received data for request (%s):\n%s" % (request, pprint.pformat(json.loads(data))))
        encoding = request.getHeader("accept-encoding")
        if encoding and "gzip" in encoding:
            zbuf = cStringIO.StringIO()
            zfile = gzip.GzipFile(None, 'wb', 9, zbuf)
            if isinstance(data, unicode):
                zfile.write(unicode(data).encode("utf-8"))
            elif isinstance(data, str):
                zfile.write(unicode(data, 'utf-8').encode("utf-8"))
            else:
                zfile.write(unicode(data).encode("utf-8"))
            zfile.close()
            request.setHeader("Content-encoding","gzip")
            request.write(zbuf.getvalue())
        else:
            request.write(data)
        request.finish()

