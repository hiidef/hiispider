import pprint
import logging
from twisted.spread import pb


LOGGER = logging.getLogger(__name__)

class Job(object, pb.Copyable, pb.RemoteCopy):

    mapped = False
    fast_cache = None
    uuid = None

    def __init__(self,
            function_name,
            service_credentials,
            user_account,
            uuid=None):
        """An encaspulated job object.  The ``function_name`` is its path on
        the http interface of the spider, the ``service_credentials`` (called
        ``kwargs`` on the job) are colums from the ``content_(service)account``
        table, and the ``user_account`` is a row of the ``spider_service``
        table along with the user's flavors username and chosen DNS host."""
        self.function_name = function_name
        self.kwargs = service_credentials
        self.subservice = function_name
        self.uuid = uuid
        self.user_account = user_account
        self.dotted_name = function_name.replace("/", ".")

    def __repr__(self):
        return '<job: %s(%s)>' % (self.function_name, self.uuid)

    def __str__(self):
        return 'Job %s: \n%s' % (self.uuid, pprint.pformat(self.__dict__))

#class Job(BaseJob, pb.Copyable):
#    pass
#
#class JobRemoteCopy(pb.RemoteCopy, BaseJob):
#    pass

pb.setUnjellyableForClass(Job, Job) 
