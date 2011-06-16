
import cPickle 
from twisted.internet.defer import inlineCallbacks, returnValue
from zlib import compress, decompress
from .mysql import MySQLMixin
from ..base import LOGGER, Job

class JobGetterMixin(MySQLMixin):
    
    def setupJobGetter(self, config):
        self.setupMySQL(config)
    
    @inlineCallbacks  
    def getJob(self, uuid):
        try:
            job = yield self._getCachedJob(uuid)
            returnValue(job)
        except:
            pass
        user_account = yield self._getUserAccount(uuid)
        service_type = user_account['type'].split('/')[0].lower()
        account_id = user_account['account_id']
        service_credentials = yield self._getServiceCredentials(service_type, account_id)
        job = Job(
            function_name=user_account['type'],
            uuid=uuid,
            service_credentials=service_credentials,
            user_account=user_account,
            functions=self.functions,
            service_mapping=self.service_mapping,
            service_args_mapping=self.service_args_mapping)
        self._setJobCache(job)
        returnValue(job)
    
    @inlineCallbacks    
    def _getUserAccount(self, uuid):
        sql = """SELECT content_userprofile.user_id as user_id, username, host, account_id, type
            FROM spider_service, auth_user, content_userprofile
            WHERE uuid = '%s'
            AND auth_user.id=spider_service.user_id
            AND auth_user.id=content_userprofile.user_id
        """ % uuid
        try:
            data = yield self.mysql.runQuery(sql)
        except Exception, e:
            LOGGER.debug("Could not find user %s" % uuid)
            raise e
        if len(data) == 0: # No results?
            message = "Could not find user %s: %s" % uuid
            LOGGER.error(message)
            raise Exception(message)
        returnValue(data[0])
        
    @inlineCallbacks  
    def _getServiceCredentials(self, service_type, account_id):
        sql = "SELECT * FROM content_%saccount WHERE account_id = %d" % (service_type, account_id)
        try:
            data = yield self.mysql.runQuery(sql)
        except Exception, e:
            message = "Could not find service %s:%s, %s" % (service_type, account_id)
            raise e
        if len(data) == 0: # No results?
            message = "Could not find service %s:%s" % (service_type, account_id)
            LOGGER.error(message)
            raise Exception(message)
        returnValue(data[0])
        
    @inlineCallbacks
    def _getCachedJob(self, uuid):
        """Search for job info in redis cache."""
        try:
            data = yield self.redis_client.get(uuid)
            if data:
                job = cPickle.loads(decompress(data))
                LOGGER.debug('Found uuid in Redis: %s' % uuid)
                returnValue(job)
        except Exception, e:
            LOGGER.debug('Could not find uuid in Redis: %s' % e)
            raise e
        raise Exception('Could not find uuid in Redis: %s' % e)
        
    @inlineCallbacks
    def _setJobCache(self, job):
        """Set job cache in redis. Expires at now + 7 days."""
        job_data = compress(cPickle.dumps(job), 1)
        # TODO: Figure out why txredisapi thinks setex doesn't like sharding.
        try:
            yield self.redis_client.set(job.uuid, job_data)
            yield self.redis_client.expire(job.uuid, 60*60*24*7)
        except Exception, e:
            LOGGER.error(str(e))