#!/usr/bin/env python
# encoding: utf-8
import redis
import warnings
import time

warnings.filterwarnings('ignore', category=DeprecationWarning)

r = redis.Redis(host='127.0.0.1', port=6379, db=0)
keys = r.keys('*')
for key in keys:
    if key.find(':') == -1:
        print('Deleting key: %s' % key)
        r.delete(key)
        time.sleep(0.5)
