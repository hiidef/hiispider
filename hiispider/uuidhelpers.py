#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import uuid
import time
import struct

_number_types = frozenset((int, long, float))

"""Shamelessly stolen from Pycassa.
https://github.com/pycassa/pycassa
https://github.com/pycassa/pycassa/blob/master/pycassa/util.py
"""

def convert_time_to_uuid(time_arg, lowest_val=True, randomize=False):
    """
    Converts a datetime or timestamp to a type 1 :class:`uuid.UUID`.

    This is to assist with getting a time slice of columns or creating
    columns when column names are ``TimeUUIDType``. Note that this is done
    automatically in most cases if name packing and value packing are
    enabled.

    Also, be careful not to rely on this when specifying a discrete
    set of columns to fetch, as the non-timestamp portions of the
    UUID will be generated randomly. This problem does not matter
    with slice arguments, however, as the non-timestamp portions
    can be set to their lowest or highest possible values.

    :param datetime:
      The time to use for the timestamp portion of the UUID.
      Expected inputs to this would either be a :class:`datetime`
      object or a timestamp with the same precision produced by
      :meth:`time.time()`. That is, sub-second precision should
      be below the decimal place.
    :type datetime: :class:`datetime` or timestamp

    :param lowest_val:
      Whether the UUID produced should be the lowest possible value
      UUID with the same timestamp as datetime or the highest possible
      value.
    :type lowest_val: bool

    :param randomize:
      Whether the clock and node bits of the UUID should be randomly
      generated.  The `lowest_val` argument will be ignored if this
      is true.
    :type randomize: bool

    :rtype: :class:`uuid.UUID`

    """
    if isinstance(time_arg, uuid.UUID):
        return time_arg

    nanoseconds = 0
    if hasattr(time_arg, 'timetuple'):
        seconds = int(time.mktime(time_arg.timetuple()))
        microseconds = (seconds * 1e6) + time_arg.time().microsecond
        nanoseconds = microseconds * 1e3
    elif type(time_arg) in _number_types:
        nanoseconds = int(time_arg * 1e9)
    else:
        raise ValueError('Argument for a v1 UUID column name or value was ' +
                'neither a UUID, a datetime, or a number')

    # 0x01b21dd213814000 is the number of 100-ns intervals between the
    # UUID epoch 1582-10-15 00:00:00 and the Unix epoch 1970-01-01 00:00:00.
    timestamp = int(nanoseconds/100) + 0x01b21dd213814000L

    time_low = timestamp & 0xffffffffL
    time_mid = (timestamp >> 32L) & 0xffffL
    time_hi_version = (timestamp >> 48L) & 0x0fffL

    if randomize:
        rand_bits = random.getrandbits(8 + 8 + 48)
        clock_seq_low = rand_bits & 0xffL # 8 bits, no offset
        clock_seq_hi_variant = (rand_bits & 0xff00L) / 0x100 # 8 bits, 8 offset
        node = (rand_bits & 0xffffffffffff0000L) / 0x10000L # 48 bits, 16 offset
    else:
        if lowest_val:
            # Make the lowest value UUID with the same timestamp
            clock_seq_low = 0 & 0xffL
            clock_seq_hi_variant = 0 & 0x3fL
            node = 0 & 0xffffffffffffL # 48 bits
        else:
            # Make the highest value UUID with the same timestamp
            clock_seq_low = 0xffL
            clock_seq_hi_variant = 0x3fL
            node = 0xffffffffffffL # 48 bits
    return uuid.UUID(fields=(time_low, time_mid, time_hi_version,
                        clock_seq_hi_variant, clock_seq_low, node), version=1)

def convert_uuid_to_time(uuid_arg):
    """
    Converts a version 1 :class:`uuid.UUID` to a timestamp with the same precision
    as :meth:`time.time()` returns.  This is useful for examining the
    results of queries returning a v1 :class:`~uuid.UUID`.

    :param uuid_arg: a version 1 :class:`~uuid.UUID`

    :rtype: timestamp

    """
    ts = uuid_arg.get_time()
    return (ts - 0x01b21dd213814000L)/1e7
