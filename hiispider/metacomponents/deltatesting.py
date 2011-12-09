#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import logging
import zlib
import ujson as json
from copy import copy
from telephus.cassandra.c08.ttypes import NotFoundException
from twisted.internet.defer import inlineCallbacks, returnValue
from hiispider.components import *
from hiispider.metacomponents import *
import logging
from hiiguid import HiiGUID
from zlib import decompress
from difflib import SequenceMatcher
import binascii
from pprint import pformat
from .base import MetaComponent

LOGGER = logging.getLogger(__name__)


class DeltaTesting(MetaComponent):

    allow_clients = False
    requires = [Cassandra]

    def __init__(self, server, config, server_mode, **kwargs):
        super(DeltaTesting, self).__init__(server, server_mode)
        self.config = copy(config)
        self.service_mapping = config["service_mapping"]
        self.server.expose(self.regenerate_by_uuid)

    @inlineCallbacks
    def regenerate_by_uuid(self, uuid):
        delta = yield self.server.cassandra.get_delta(uuid)
        function_name = self.service_mapping.get(
            delta["subservice"],
            delta["subservice"])
        delta_func = self.server.functions[function_name]["delta"]
        if not delta_func:
            raise Exception("Delta function %s "
                "does not exist." % delta["service"])
        deltas = delta_func(delta["new_data"], delta["old_data"])
        if not deltas:
            raise Exception("No deltas were generated.")
        # Find nearest delta.
        delta_options = []
        s = SequenceMatcher()
        s.set_seq1(json.dumps(delta["data"]))
        for delta in deltas:
            LOGGER.debug(pformat(delta.data))
            value = json.dumps(delta.data)
            s.set_seq2(value)
            delta_options.append((s.ratio(), value, delta.data))
        # Sort to find the most similar option.
        delta_options = sorted(delta_options, key=lambda x: x[0])
        replacement_data = zlib.compress(json.dumps(delta_options[-1][2]))
        ts = str(time.time())
        mapping = {"updated": ts, "data": replacement_data}
        yield self.server.cassandra.batch_insert(
            key=binascii.unhexlify(uuid),
            column_family=self.server.cassandra.cf_delta,
            mapping=mapping,
            consistency=2)
        LOGGER.debug("%s deltas generated." % len(deltas))
        returnValue(True)
