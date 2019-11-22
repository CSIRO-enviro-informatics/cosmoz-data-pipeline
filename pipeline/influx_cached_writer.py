# -*- coding: utf-8 -*-
#
import time

from influxdb.exceptions import InfluxDBServerError


class AccumCacheInfluxWriter(object):
    __slots__ = ('influx_client', 'accum_cache', 'current_len', 'cache_len')

    def __new__(cls, influx_client, cache_length=10):
        self = super(AccumCacheInfluxWriter, cls).__new__(cls)
        self.influx_client = influx_client
        self.accum_cache = []
        self.current_len = 0
        self.cache_len = cache_length
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if self.current_len > 0:
            self._maybe_flush(force=True, delete_cache=True)

    def _maybe_flush(self, force=False, delete_cache=False):
        if force or self.current_len >= self.cache_len:
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
            self.current_len = 0

    def write_point(self, point, *args, **kwargs):
        self.accum_cache.append(point)
        self.current_len += 1
        self._maybe_flush()

    def write_points(self, points, *args, **kwargs):
        self.accum_cache.extend(points)
        self.current_len += len(points)
        self._maybe_flush()
