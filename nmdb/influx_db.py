# -*- coding: utf-8 -*-
#
import math
import time

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError

from .intensity import Intensity
from .utils import datetime_to_isostring, isostring_to_datetime
from ._influx_db_config import config as influx_config

INTENSITY_TABLE = "intensity"
RAW_VALS_TABLE = "raw_values"

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


def make_influx_client(new=False):
    if not new and make_influx_client.cached:
        return make_influx_client.cached
    if new:
        make_influx_client.cached = None
    influx_client = InfluxDBClient(
        influx_config['DB_HOST'], int(influx_config['DB_PORT']),
        influx_config['DB_USERNAME'], influx_config['DB_PASSWORD'],
        influx_config['DB_NAME'], timeout=30)
    make_influx_client.cached = influx_client
    return influx_client
make_influx_client.cached = None


def store_raw_data(raw_data_rows, influx_client=None):
    if influx_client is None:
        influx_client = make_influx_client()
    rows = iter(raw_data_rows)
    with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
        while True:
            try:
                row = next(rows)
            except (IndexError, StopIteration):
                break
            if row is None or row is False:
                break
            site_num = int(row.site_no)
            iso_timestamp = datetime_to_isostring(row.current_timestamp)
            if site_num < 1:
                continue
            json_body = {
                "measurement": RAW_VALS_TABLE,
                "tags": {
                    "site_no": site_num,
                    "flag": 0,  # there is no way to set a flag using this code
                },
                "time": iso_timestamp,  # "2009-11-10T23:00:00Z",
                "fields": {
                    "count": int(math.floor(row.fast)),
                    "pressure1": float(row.pressure),
                    "internal_temperature": float(row.temperature),
                    "internal_humidity": float(row.humidity),
                    "battery": float(row.voltage),
                    "tube_temperature": float(row.tube_cap_temp),
                    "tube_humidity": float(row.tube_cap_humi),
                    "rain": float(row.rain_gauge),
                    "vwc1": float(row.vwc_sens1),
                    "vwc2": float(row.vwc_sens2),
                    "vwc3": float(row.vwc_sens3),
                    "pressure2": float(row.pressure2),
                    "external_temperature": float(row.temperature2),
                    "external_humidity": float(row.humidity2),
                }
            }
            writer.write_point(json_body)


def store_intensity_data(intensity_data_rows, influx_client=None):
    if influx_client is None:
        influx_client = make_influx_client()
    rows = iter(intensity_data_rows)
    with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
        while True:
            try:
                row = next(rows)
            except (IndexError, StopIteration):
                break
            if row is None or row is False:
                break
            bad_data = 0 if row.bad_data_flag is None else int(row.bad_data_flag)
            site_num = int(row.site_no)
            iso_timestamp = datetime_to_isostring(row.timestamp)
            if site_num < 1:
                continue
            json_body = {
                "measurement": INTENSITY_TABLE,
                "tags": {
                    "site_no": site_num,
                    "bad_data_flag": bad_data,
                },
                "time": iso_timestamp,  # "2009-11-10T23:00:00Z",
                "fields": {
                    "intensity": float(row.intensity_value)
                }
            }
            writer.write_point(json_body)


def get_intensity_timestamp(site_no, influx_client=None):
    """
    Queries the database to retrieve the last valid timestamp
    :param site_no: The siteNo in the Intensity table to check for
    :param influx_client:
    :return: The last valid Intensity timestamp for this site
    :rtype: datetime.datetime
    """
    timestamp = _get_max_intensity_timestamp(site_no, influx_client)
    if timestamp is None:
        timestamp = _get_first_data_timestamp(site_no, influx_client)
    return timestamp


def _get_max_intensity_timestamp(site_no, influx_client=None):
    """
    A private method returns the maximum timestamp in the Intensity table for a given siteNo.
    :param site_no: The siteNo in the Intensity table to check for
    :param influx_client:
    :return: The maximum timestamp as a new datetime
    :rtype: datetime
    """
    try:
        if influx_client is None:
            influx_client = make_influx_client()
        select_query = """SELECT * FROM "{}" WHERE site_no='{}' ORDER BY time DESC LIMIT 1""".format(INTENSITY_TABLE, site_no)
        #String selectQuery = "SELECT MAX(Timestamp) FROM " + INTENSITY_TABLE + " WHERE SiteNo = " + siteNo;

        #Statement st = connection.createStatement();
        #st.execute(selectQuery);
        #ResultSet rs = st.getResultSet();
        #rs.next();
        response = influx_client.query(select_query)
        try:
            intensities = list(response.get_points())
            assert len(intensities) > 0
            if len(intensities) > 1:
                print("Found too many intensity records in a single hour period.")
            intensity_p = intensities[0]
        except (StopIteration, AssertionError):
            print(RuntimeError("Cannot get max (latest) intensity time for site {}".format(site_no)))
            raise
        max_timestamp_as_string = str(intensity_p['time'])
        max_timestamp = isostring_to_datetime(max_timestamp_as_string)
        return max_timestamp
    except Exception as e:
        print(e)
        # try {
        #     writer.write("Couldn't get maximum timestamp for SiteNo: " + siteNo +"!\n");
        #     writer.write("Cause: " + e.getMessage() + "\n");
        #     writer.write("Stack Trace:\n");
        #     for (int i = 0; i < e.getStackTrace().length; i++)
        #         writer.write(e.getStackTrace()[i].toString() + "\n");
        # } catch (IOException ioe) {}
        return None


def _get_first_data_timestamp(site_no, truncate_hour=True, influx_client=None):
    """
    A private method returns the first ever timestamp in the Intensity table for a given siteNo.
    :param site_no: The siteNo in the Intensity table to check for
    :param truncate_hour:
    :param influx_client:
    :return: The first timestamp as a new datetime
    :rtype: datetime
    """
    try:
        if influx_client is None:
            influx_client = make_influx_client()
        select_query = """SELECT * FROM "{}" WHERE site_no='{}' ORDER BY time ASC LIMIT 1""".format(RAW_VALS_TABLE, site_no)
        # Statement st = connection.createStatement();
        # st.execute(selectQuery);
        # ResultSet rs = st.getResultSet();
        # rs.next();

        response = influx_client.query(select_query)
        try:
            intensities = list(response.get_points())
            assert len(intensities) > 0
            if len(intensities) > 1:
                print(
                    "Found too many data records in a single hour period.")
            intensity_p = intensities[0]
        except (StopIteration, AssertionError):
            print(RuntimeError(
                "Cannot get first data time for site {}".format(site_no)))
            raise
        first_timestamp_string = str(intensity_p['time'])
        first_timestamp = isostring_to_datetime(first_timestamp_string)
        if truncate_hour:
            first_timestamp = first_timestamp.replace(minute=0, second=0, microsecond=0)
        return first_timestamp
    except Exception as e:
        print(e)
        # try:
        #     writer.write("Couldn't get first ever timestamp for SiteNo: " + siteNo +"!\n");
        #     writer.write("Cause: " + e.getMessage() + "\n");
        #     writer.write("Stack Trace:\n");
        #     for (int i = 0; i < e.getStackTrace().length; i++)
        #         writer.write(e.getStackTrace()[i].toString() + "\n");
        # except IOError:
        #     pass
        return None


def get_previous_valid_intensity_row(site_no, before_time, influx_client=None):
    """
    Queries the database to retrieve all data about the last valid
    Intensity record.

    :param site_no: The siteNo in the Intensity table to check for
    :param before_time:
    :param influx_client:
    :return: A new Intensity object containing data from the database or None on failure
    :rtype: Intensity | NoneType
    """
    try:
        if influx_client is None:
            influx_client = make_influx_client()
        before_time_string = datetime_to_isostring(before_time)
        select_query = """SELECT time,intensity FROM "{}" WHERE time<='{}' AND site_no='{}' AND bad_data_flag='0' ORDER BY time DESC LIMIT 1""".format(INTENSITY_TABLE, before_time_string, site_no)
        # String selectQuery = "SELECT TOP 1 Timestamp, Intensity "
        #     + "FROM " + INTENSITY_TABLE + " WHERE SiteNo = " + siteNo + " AND BadDataFlag = 0 "
        #         + " ORDER BY Timestamp DESC";
        response = influx_client.query(select_query)
        try:
            intensities = list(response.get_points())
            assert len(intensities) > 0
            if len(intensities) > 1:
                print(
                    "Found too many intensity records in a single hour period.")
            intensity_p = intensities[0]
        except (StopIteration, AssertionError):
            print(RuntimeError("Cannot get recent valid intensity time for site {}".format(site_no)))
            raise
        timestamp_as_string = str(intensity_p['time'])
        intensity_value = float(intensity_p['intensity'])
        timestamp = isostring_to_datetime(timestamp_as_string)
        return Intensity(site_no, timestamp, intensity_value, 0);
    except Exception as e:
        print(e)
        # try {
        #     writer.write("Couldn't get valid intensity SiteNo: " + siteNo +"!\n");
        #     writer.write("Cause: " + e.getMessage() + "\n");
        #     writer.write("Stack Trace:\n");
        #     for (int i = 0; i < e.getStackTrace().length; i++)
        #         writer.write(e.getStackTrace()[i].toString() + "\n");
        # } catch (IOException ioe) {}
        return None
