#!/usr/bin/env python
# encoding: utf-8
import redis
import pickle
from zlib import decompress
import warnings
import time

warnings.filterwarnings('ignore', category=DeprecationWarning)

r = redis.Redis(host='127.0.0.1', port=6379, db=0)
keys = r.keys('negative_req_cache:*')
for key in keys:
    redis_data = r.get(key)
    try:
        data = pickle.loads(str(decompress(redis_data)))
    except Exception, e:
        print e
    print('key: %s' % key)
    if 'status' in dir(data['error'].value):
        print('status: %s\nmsg: %s' % (data['error'].value.status, data['error'].value.message))
    else:
        print('traceback:')
        data['error'].printBriefTraceback()
    expires = time.ctime(data['timeout'])
    print('expires-at: %s\n' % expires)
