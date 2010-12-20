#!/usr/bin/env python
# encoding: utf-8
import redis
import pickle
from zlib import decompress
import warnings
import time

warnings.filterwarnings('ignore', category=DeprecationWarning)

r = redis.Redis(host='127.0.0.1', port=6379, db=0)
key = 'negative_cache:twitter.com'
redis_data = r.get(key)
if redis_data:
    data = pickle.loads(str(decompress(redis_data)))
    host = key.split(':')[1]
    print('host: %s' % host)
    if 'status' in dir(data['error'].value):
        print('status: %s\nmsg: %s' % (
            data['error'].value.status,
            data['error'].value.message,
        ))
    else:
        print('traceback:')
        data['error'].printDetailedTraceback()
    expires = time.ctime(data['timeout'])
    print('expires-at: %s\n' % expires)
else:
    print('%s is not in the negative cache' % key)
