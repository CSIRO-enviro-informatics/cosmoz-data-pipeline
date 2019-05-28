# -*- coding: utf-8 -*-
#
import time

from influxdb.exceptions import InfluxDBServerError


class AccumCacheInfluxWriter(object):
    def __new__(cls, influx_client, cache_length=10):
        self = super(AccumCacheInfluxWriter, cls).__new__(cls)
        self.influx_client = influx_client
        self.accum_cache = []
        self.cache_length = cache_length
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if len(self.accum_cache):
            self._maybe_flush(force=True, delete_cache=True)

    def _maybe_flush(self, force=False, delete_cache=False):
        if force or len(self.accum_cache) >= self.cache_length:
            try:
                self.influx_client.write_points(self.accum_cache)
            except InfluxDBServerError as er:
                msg = er.args[0]
                if b'"timeout"' in msg:
                    time.sleep(5)
                    self.influx_client.write_points(self.accum_cache)
                else:
                    raise
            if delete_cache:
                del self.accum_cache
            else:
                self.accum_cache = []

    def write_point(self, point, *args, **kwargs):
        self.accum_cache.append(point)
        self._maybe_flush()

    def write_points(self, points, *args, **kwargs):
        self.accum_cache.extend(points)
        self._maybe_flush()
