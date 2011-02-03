#!/usr/bin/env python
# encoding: utf-8
import redis
import warnings
from time import sleep

warnings.filterwarnings('ignore', category=DeprecationWarning)

r = redis.Redis(host='127.0.0.1', port=6379, db=0)
keys = r.keys('negative_req_cache:*')
print('Removing %d keys' % len(keys))
for key in keys:
    sleep(0.5)
    r.delete(key)
