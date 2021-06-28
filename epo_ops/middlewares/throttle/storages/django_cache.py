# -*- coding: utf-8 -*-
from __future__ import division

import logging
import os
import re
from datetime import timedelta
from itertools import cycle

from dateutil.parser import parse

from ....utils import makedirs, now
from .storage import Storage

from django.core.cache import cache

log = logging.getLogger(__name__)


def convert_timestamp(ts):
    return parse(ts)


class DjangoCache(Storage):
    SERVICES = ("images", "inpadoc", "other", "retrieval", "search")

    def parse_throttle(self, throttle):
        re_str = r"{0}=(\w+):(\d+)"
        status = {"services": {}}
        status["system_status"] = re.search("^(\\w+) \\(", throttle).group(1)
        for service in self.SERVICES:
            match = re.search(re_str.format(service), throttle)
            status["services"][service] = {
                "status": match.group(1),
                "limit": int(match.group(2)),
            }
        return status

    def delay_for(self, service):
        "This method is a public interface for a throttle storage class"

        _now = now()

        r = cache.get("epo_ops_throttle_data")

        if not r:  # If there is no data
            next_run = _now
        elif r["status"]["services"][service]["limit"] == 0:
            next_run = r["timestamp"] + timedelta(milliseconds=r["retry_after"])
        else:
            next_run = _now + timedelta(
                seconds=60.0 / r["status"]["services"][service]["limit"]
            )

        if next_run < _now:
            return 0.0
        else:
            td = next_run - _now
            ts = td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6
            return ts / 10 ** 6

    def update(self, headers):
        "This method is a public interface for a throttle storage class"

        if "x-throttling-control" not in headers:
            return
        status = self.parse_throttle(headers["x-throttling-control"])
        retry_after = int(headers.get("retry-after", 0))
        cache.set(
            "epo_ops_throttle_data",
            {"retry_after": retry_after, "status": status, "timestamp": now()},
        )
