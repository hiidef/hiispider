#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Hiispider Exceptions"""

class DeleteReservationException(Exception):
    pass

class StaleContentException(Exception):
    pass

class NegativeCacheException(Exception):
    pass

class QueueTimeoutException(Exception):
    pass

class NegativeHostCacheException(NegativeCacheException):
    pass

class NegativeReqCacheException(NegativeCacheException):
    pass

class JobGetterShutdownException(Exception):
    pass

class NotRunningException(Exception):
	pass