#!/usr/bin/env python
# encoding: utf-8
import redis


r = redis.Redis(host='127.0.0.1', port=6379, db=0)
num_negative_cache_keys = len(r.keys('negative_cache:*'))
num_negative_req_cache_keys = len(r.keys('negative_req_cache:*'))
num_all_keys = len(r.keys('*'))
num_account_cache_keys = num_all_keys - \
    num_negative_cache_keys - \
    num_negative_req_cache_keys
print('nc: %d, nrc: %d, ac: %d, t: %d' % (
    num_negative_cache_keys,
    num_negative_req_cache_keys,
    num_account_cache_keys,
    num_all_keys,
))
