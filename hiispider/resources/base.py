from twisted.python.failure import Failure
from twisted.web.resource import Resource
import cStringIO, gzip
import traceback
import simplejson
import logging

logger = logging.getLogger(__name__)

class BaseResource(Resource):

    def __init__(self):
        Resource.__init__(self)

    def _successResponse(self, data):
        # if isinstance(data, str):
        #      return data
        return simplejson.dumps(data)

    def _errorResponse(self, error):
        from ..servers.base import LOGGER
        reason = str(error.value)
        # there are two ways to extract a traceback;  we pick whichever one
        # is longer as that probably has more information
        tbs = (error.getTraceback(),
               traceback.format_exc(traceback.extract_tb(error.tb)))
        tbs.sort(key=lambda x: len(x), reverse=True)
        tb = tbs[0]
        LOGGER.error("%s\n%s\n%s" % (reason, tb))
        return simplejson.dumps({"error":reason, "traceback":tb})

    def _immediateResponse(self, data, request):
        logger.debug("%s received for %s" % (data, request))
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
