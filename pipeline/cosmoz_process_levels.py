#!/bin/python3
# -*- coding: utf-8 -*-
"""
Copyright 2019 CSIRO Land and Water

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import argparse
import sys
from multiprocessing import Process, Pool, Queue
import math
from datetime import time as d_time, datetime, timedelta, timezone
from influxdb import InfluxDBClient
from pymongo import MongoClient
from sortedcontainers import SortedList

from .influx_cached_writer import AccumCacheInfluxWriter
from .utils import datetime_to_isostring, isostring_to_datetime
from ._influx_db_config import config as influx_config
from ._mongo_db_config import config as mongodb_config

influx_client = InfluxDBClient(
    influx_config['DB_HOST'], int(influx_config['DB_PORT']),
    influx_config['DB_USERNAME'], influx_config['DB_PASSWORD'],
    influx_config['DB_NAME'], timeout=30)

THIRTY_YEARS = timedelta(days=365 * 30)
TEN_YEARS = timedelta(days=365 * 10)
ONE_YEAR = timedelta(days=365)


def level3_to_level4(site_no=1, start_time=None, backprocess=None, drop_old=False):
    if start_time is None:
        start_time = datetime.now().astimezone(timezone.utc)
    if backprocess is None:
        backprocess = TEN_YEARS
    back_time = start_time - backprocess
    time_string = datetime_to_isostring(back_time)
    result = influx_client.query("""\
SELECT "time", site_no, soil_moist, effective_depth, rainfall
--FROM "level3_temp"
FROM "level3"
WHERE "time" > '{}' AND flag='0' AND site_no='{}'""".format(time_string, site_no))
    points = result.get_points()
    if drop_old:
        influx_client.query(
            "DROP SERIES FROM level4 WHERE site_no='{}';".format(site_no),
            method='POST')
    with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
        for p in points:
            this_datetime = isostring_to_datetime(p['time'])
            three_h_ago = datetime_to_isostring(this_datetime - timedelta(hours=3, seconds=1))
            three_h_fwd = datetime_to_isostring(this_datetime + timedelta(hours=3, seconds=1))

            r2 = influx_client.query("""\
SELECT MEAN("soil_moist") as soil_moist_filtered, MEAN("effective_depth") as depth_filtered
FROM (SELECT soil_moist, effective_depth FROM "level3"
WHERE "time" >= '{}' AND "time" <= '{}' AND flag='0' AND site_no='{}'
LIMIT 7)""".format(three_h_ago, three_h_fwd, site_no))
            p2 = r2.get_points()
            try:
                avgs = next(p2)
                soil_moist_filtered = avgs['soil_moist_filtered']
                depth_filtered = avgs['depth_filtered']
            except (StopIteration, KeyError):
                soil_moist_filtered = p['soil_moist']
                depth_filtered = p['effective_depth']
            json_body = {
                "measurement": "level4",
                #"measurement": "level4_temp",
                "tags": {
                    "site_no": p['site_no'],
                },
                "time": p['time'],
                "fields": {
                    "soil_moist": float(p['soil_moist']),
                    "effective_depth": float(p['effective_depth']),
                    "rainfall": float(p['rainfall']),
                    "soil_moist_filtered": soil_moist_filtered,
                    "depth_filtered": depth_filtered
                }
            }
            writer.write_point(json_body)


def level2_to_level3(mongo_client, site_no=1, start_time=None, backprocess=None, drop_old=False):
    if start_time is None:
        start_time = datetime.now().astimezone(timezone.utc)
    if backprocess is None:
        backprocess = TEN_YEARS
    back_time = start_time - backprocess
    time_string = datetime_to_isostring(back_time)
    mdb = getattr(mongo_client, mongodb_config['DB_NAME'])
    all_stations = mdb.all_stations
    this_site = all_stations.find_one({'site_no': site_no})
    try:
        alternate_algorithm = this_site["alternate_algorithm"]
    except LookupError:
        alternate_algorithm = None
    sandy_a = 1216036430.0
    sandy_b = -3.272
    result = influx_client.query("""\
SELECT "time", site_no, wv_corr, corr_count, rain, flag as level2_flag
--SELECT "time", site_no, wv_corr, corr_count, flag as level2_flag
--FROM "level2_temp"
FROM "level2"
WHERE "time" > '{}' AND site_no='{}'""".format(time_string, site_no))
    points = result.get_points()
    if drop_old:
        influx_client.query("DROP SERIES FROM level3 WHERE site_no='{}';".format(site_no), method='POST')
    with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
        for p in points:
            wv_corr = float(p['wv_corr'])
            corr_count = float(p['corr_count'])
            n0_cal = float(this_site['n0_cal'].to_decimal())
            bulk_density = float(this_site['bulk_density'].to_decimal())
            lattice_water_g_g = this_site['lattice_water_g_g'].to_decimal()
            soil_organic_matter_g_g = this_site['soil_organic_matter_g_g'].to_decimal()
            lattice_soil_organic_sum = float(lattice_water_g_g + soil_organic_matter_g_g)
            if alternate_algorithm and alternate_algorithm == "sandy":
                if wv_corr == 1.0:
                    flag = 5
                elif corr_count > (3.0 * n0_cal):
                     flag = 3
                elif corr_count < (0.5 * n0_cal):
                    flag = 2
                else:
                    flag = int(p['level2_flag'])
                corrected_moist_val = sandy_a * (corr_count ** sandy_b)
            else:
                if wv_corr == 1.0:
                    flag = 5
                elif corr_count > n0_cal:
                    flag = 3
                elif corr_count < (0.4 * n0_cal):
                    flag = 2
                else:
                    flag = int(p['level2_flag'])
                corrected_moist_val = (0.0808 / ((corr_count / n0_cal) - 0.372) - 0.115 - lattice_soil_organic_sum) * bulk_density
            #((0.0808 / ((l2.CorrCount / a.N0_Cal) - 0.372) - 0.115 - a.LatticeWater_g_g - a.SoilOrganicMatter_g_g) * a.BulkDensity) * 100
            soil_moisture = corrected_moist_val * 100.0
            #5.8 / ( ((a.LatticeWater_g_g + a.SoilOrganicMatter_g_g) * a.BulkDensity) + ( (0.0808 / ( (l2.CorrCount / a.N0_Cal) - 0.372) - 0.115 - a.LatticeWater_g_g - a.SoilOrganicMatter_g_g) * a.BulkDensity ) + 0.0829) AS EffectiveDepth,
            effective_depth = 5.8 / ((lattice_soil_organic_sum * bulk_density) + corrected_moist_val + 0.0829)
            json_body = {
                "measurement": "level3",
                #"measurement": "level3_temp",
                "tags": {
                    "site_no": p['site_no'],
                    "flag": flag,
                },
                "time": p['time'],
                "fields": {
                    "soil_moist": soil_moisture,
                    "effective_depth": effective_depth,
                    "rainfall": float(p['rain']) * 0.2
                }
            }
            writer.write_point(json_body)


def level1_to_level2(mongo_client, site_no=1, start_time=None, backprocess=None, drop_old=False):
    emulate_old_version = False
    if start_time is None:
        start_time = datetime.now().astimezone(timezone.utc)
    if backprocess is None:
        backprocess = TEN_YEARS
    back_time = start_time - backprocess
    time_string = datetime_to_isostring(back_time)
    mdb = getattr(mongo_client, mongodb_config['DB_NAME'])
    all_stations_collection = mdb.all_stations
    this_site = all_stations_collection.find_one({'site_no': site_no})
    result = influx_client.query("""\
SELECT "time", site_no, "count", pressure1, pressure2, external_temperature, external_humidity, rain, flag as level1_flag
FROM "level1"
WHERE "time" > '{}' AND site_no=$s""".format(time_string), bind_params={"s": str(site_no)})
    points = result.get_points()
    if drop_old:
        influx_client.query("DROP SERIES FROM level2 WHERE site_no=$s;", bind_params={"s": str(site_no)}, method='POST')
    with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
        for p in points:
            count = p['count']
            pressure1 = float(p['pressure1'])
            pressure2 = float(p['pressure2'])
            if pressure2 != 0:
                press_corr = math.exp(float(this_site['beta'].to_decimal()) * (pressure2 - float(this_site['ref_pressure'].to_decimal())))
            elif pressure1 != 0:
                press_corr = math.exp(float(this_site['beta'].to_decimal()) * (pressure1 - float(this_site['ref_pressure'].to_decimal())))
            else:
                press_corr = 1.0
            this_datetime = isostring_to_datetime(p['time'])
            this_day_start = datetime_to_isostring(this_datetime.date())
            this_day_end = datetime_to_isostring(datetime.combine(this_datetime.date(), d_time(11, 59, 59, 999999, tzinfo=this_datetime.tzinfo)))
            this_hour_start = datetime_to_isostring(datetime.combine(this_datetime.date(), d_time(this_datetime.hour, 0, 0, 0, tzinfo=this_datetime.tzinfo)))
            this_hour_end = datetime_to_isostring(datetime.combine(this_datetime.date(), d_time(this_datetime.hour, 59, 59, 999999, tzinfo=this_datetime.tzinfo)))
            external_temperature = float(p['external_temperature'])
            external_humidity = float(p['external_humidity'])
            # if external temperature or external humidity is zero, we will need to get the data from SILO.
            if external_temperature == 0 or external_humidity == 0:
                silo_req = influx_client.query("""SELECT LAST(*) FROM "silo_data" WHERE "time" >= '{}' AND "time" <= '{}' AND site_no='{}'""".format(this_day_start, this_day_end, site_no))
                try:
                    sp = next(silo_req.get_points())
                    average_temperature = float(sp['last_average_temperature'])
                    average_humidity = float(sp['last_average_humidity'])
                except (StopIteration, ValueError, KeyError):
                    average_temperature = None
                    average_humidity = None
            else:
                average_temperature = None
                average_humidity = None

            if external_temperature != 0 and external_humidity != 0:
                # IF ExternalTemperature AND ExternalHumidity (from the Level1View View) has valid data. Use them in the WVCorr equation
                wv_corr_store = 1+0.0054*((2165*((0.6108*math.exp((17.27*external_temperature)/(external_temperature+237.3)))*(external_humidity/100.0)))/(external_temperature+273.16)-0)
                wv_corr_use = wv_corr_store
            elif average_humidity is not None:
                # Otherwise, IF AverageTemperature AND AverageHumidity (from the SiloData table) has valid data. Use them in the WVCorr equation.
                use_temp = average_temperature if average_temperature is not None else 0.0
                wv_corr_use = 1+0.0054*((2165*((0.6108*math.exp((17.27*use_temp)/(use_temp+237.3)))*(average_humidity/100.0)))/(use_temp+273.16)-0)
                if emulate_old_version:
                    if average_temperature is not None:
                        wv_corr_store = wv_corr_use
                    else:
                        wv_corr_store = 1.0
                else:
                    wv_corr_store = wv_corr_use
            else:
                # Finally, use either external OR average values, or zero
                use_humidity = average_humidity if external_humidity == 0 else external_humidity
                use_temp = average_temperature if external_temperature == 0 else external_temperature
                if use_humidity is None or use_humidity == 0:
                    wv_corr_use = 1.0
                else:
                    if use_temp is None:
                        use_temp = 0.0  # Use the actual zero temp in the calculation
                    wv_corr_use = 1+0.0054*((2165*((0.6108*math.exp((17.27*use_temp)/(use_temp+237.3)))*(use_humidity/100.0)))/(use_temp+273.16)-0)
                if emulate_old_version:
                    wv_corr_store = 1.0  # Store this, to match the old way the system stored wv_corr
                else:
                    wv_corr_store = wv_corr_use
            # IF we can match the record's timestamp (to the hour) to one in the Intensity table. Use the Intensity value in the IntensityCorr equation.
            intensity_req = influx_client.query("""SELECT * FROM "intensity" WHERE "time" >= '{}' AND "time" <= '{}' AND site_no='{}'""".format(this_hour_start, this_hour_end, site_no))
            try:
                intensities = list(intensity_req.get_points())
                assert len(intensities) > 0
                if len(intensities) > 1:
                    print("Found too many intensity records in a single hour period.")
                intensity_p = intensities[0]
                #int_key = "intensity"
            except (StopIteration, AssertionError):
                # no intensity to the nearest hour
                # Otherwise, IF we can find the last valid timestamp for this record. Use the Intensity value in the IntensityCorr equation.
                if emulate_old_version:
                    intensity_req = influx_client.query("""SELECT FIRST("intensity") AS "intensity", "bad_data_flag" FROM "intensity" WHERE "time" <= '{}' AND site_no='{}'""".format(p['time'], site_no))
                    #int_key = "first_intensity"
                else:
                    intensity_req = influx_client.query("""SELECT LAST("intensity") AS "intensity", "bad_data_flag" FROM "intensity" WHERE "time" <= '{}' AND site_no='{}'""".format(p['time'], site_no))
                    #int_key = "last_intensity"
                try:
                    intensity_p = next(intensity_req.get_points())
                except StopIteration:
                    intensity_req = influx_client.query("""SELECT FIRST("intensity") AS "intensity", "bad_data_flag" FROM "intensity" WHERE "time" >= '{}' AND site_no='{}'""".format(p['time'], site_no))
                    try:
                        intensity_p = next(intensity_req.get_points())
                        #int_key = "first_intensity"
                    except Exception:
                        intensity_p = None
                        #int_key = None
            if intensity_p and "intensity" in intensity_p:
                use_intensity = float(intensity_p["intensity"])
                if use_intensity == 0.0:  # prevent div by zero
                    intensity_corr = 1.0
                else:
                    intensity_corr = use_intensity / float(this_site['ref_intensity'].to_decimal())
            else:
                intensity_corr = 1.0
            latit_scaling = this_site['latit_scaling'].to_decimal()
            elev_scaling = this_site['elev_scaling'].to_decimal()
            try:
                corr_count = (float(count)*wv_corr_use*press_corr/intensity_corr)/float(latit_scaling/elev_scaling)
            except ZeroDivisionError:
                print("count:", p["count"])
                print("latit_scaling:", latit_scaling)
                print("elev_scaling:", elev_scaling)
                print("wv_corr_use:", wv_corr_use)
                print("intensity_corr:", intensity_corr)
                raise
            json_body = {
                "measurement": "level2",
                #"measurement": "level2_temp",
                "tags": {
                    "site_no": p['site_no'],
                    "flag": int(p['level1_flag']),
                },
                "time": p['time'],
                "fields": {
                    "count": int(count),
                    "press_corr": press_corr,
                    "wv_corr": wv_corr_store,
                    "intensity_corr": intensity_corr,
                    "corr_count": corr_count,
                    "rain": float(p['rain']),
                }
            }
            writer.write_point(json_body)

def is_duplicate(site_no, record1, record2, table):
    if isinstance(record2, datetime):
        record2 = datetime_to_isostring(record2)
    if isinstance(record2, str):
        result = influx_client.query("""\
        SELECT "time", site_no, "count", pressure1, internal_temperature, internal_humidity, battery, tube_temperature, tube_humidity, rain, vwc1, vwc2, vwc3, pressure2, external_temperature, external_humidity, flag as raw_flag
        FROM "{}"
        WHERE "time" = '{}' AND site_no=$s;""".format(table, record2), bind_params={"s": str(site_no)})
        points = result.get_points()
        try:
            record2 = next(iter(points))
        except (IndexError, StopIteration):
            return False
    different = {}
    for key, val in record1.items():
        if key in ("time", "site_no", "flag"):
            continue
        if key in record2:
            val2 = record2[key]
            if val != val2:
                different[key] = (val, val2)
    return len(different) < 1


def raw_to_level1(site_no=1, start_time=None, backprocess=None, drop_old=False):
    if start_time is None:
        start_time = datetime.now().astimezone(timezone.utc)
    if backprocess is None:
        backprocess = TEN_YEARS
    back_time = start_time - backprocess
    time_string = datetime_to_isostring(back_time)
    a_res = influx_client.query("""SELECT "time", "count", site_no FROM "raw_values" WHERE site_no=$s""", bind_params={"s": str(site_no)})
    all_mapped = {}
    for p in a_res.get_points():
        all_mapped[isostring_to_datetime(p["time"])] = p
    all_times = SortedList(all_mapped.keys())
    result = influx_client.query("""\
SELECT "time", site_no, "count", pressure1, internal_temperature, internal_humidity, battery, tube_temperature, tube_humidity, rain, vwc1, vwc2, vwc3, pressure2, external_temperature, external_humidity, flag as raw_flag
FROM "raw_values"
WHERE "time" > '{}' AND site_no=$s;""".format(time_string), bind_params={"s": str(site_no)})
    points = result.get_points()
    result2 = influx_client.query("""\
SELECT DIFFERENCE("count") as count_diff
FROM "raw_values"
WHERE "time" > '{}' AND site_no=$s""".format(time_string), bind_params={"s": str(site_no)})
    points2 = result2.get_points()
    if drop_old:
        influx_client.query("DROP SERIES FROM level1 WHERE site_no=$s;", bind_params={"s":str(site_no)}, method='POST')
    with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
        try:
            #skip the first p, because it doesn't have a corresponding diff
            _ = next(points)
        except StopIteration:
            return
        #influx_client.query("DROP SERIES FROM level1_temp WHERE site_no='{}';".format(site_no), method='POST')
        for p in points:
            time_str = p['time']
            at_time = isostring_to_datetime(time_str)
            count = p['count']
            dup_back_time = at_time - timedelta(minutes=29.0)
            possible_duplicates_times = list(all_times.irange(dup_back_time, at_time, inclusive=(True, False)))
            if len(possible_duplicates_times) > 0:
                for dt in possible_duplicates_times:
                    dc = all_mapped[dt]['count']
                    dtstring = all_mapped[dt]['time']
                    if dc == count:
                        im_duplicate = is_duplicate(site_no, p, dtstring, 'raw_values')
                        if im_duplicate:
                            break
                else:
                    im_duplicate = False
                if im_duplicate:
                    print("Skipping time {} at site {} because it is a duplicate.".format(time_str, site_no))
                    _ = next(points2)
                    continue
            bat = p['battery']
            p2 = next(points2)
            count_diff = p2['count_diff']
            prev_count = count+(count_diff * -1.0)
            if prev_count < 0:
                raise ValueError("Incorrect previous_count calculation.")
            if bat < 10:
                flag = 4
            elif count < (0.8 * prev_count) or count > (1.2 * prev_count):
                flag = 1
            else:
                flag = p['raw_flag']

            json_body = {
                "measurement": "level1",
                #"measurement": "level1_temp",
                "tags": {
                    "site_no": p['site_no'],
                    "flag": int(flag),
                },
                "time": time_str,
                "fields": {
                    "count": int(p['count']),
                    "pressure1": float(p['pressure1']),
                    "internal_temperature": float(p['internal_temperature']),
                    "internal_humidity": float(p['internal_humidity']),
                    "battery": float(p['battery']),
                    "tube_temperature": float(p['tube_temperature']),
                    "tube_humidity": float(p['tube_humidity']),
                    "rain": float(p['rain']),
                    "vwc1": float(p['vwc1']),
                    "vwc2": float(p['vwc2']),
                    "vwc3": float(p['vwc3']),
                    "pressure2": float(p['pressure2']),
                    "external_temperature": float(p['external_temperature']),
                    "external_humidity": float(p['external_humidity']),
                }
            }
            writer.write_point(json_body)


def fix_raws(site_no=1):
    result = influx_client.query("""\
SELECT *
FROM "raw_values"
WHERE site_no='{}';""".format(site_no))
    points = result.get_points()
    bad_times = []
    for p in points:
        count = p['count']
        battery = p['battery']
        time = p['time']
        if count is None and battery is None:
            bad_times.append(time)
    if len(bad_times) < 1:
        return
    for bad_t in bad_times:
        del_query = """\
DELETE FROM "raw_values"
WHERE site_no='{}' AND time='{}';""".format(site_no, bad_t)
        result = influx_client.query(del_query)
        print(result)
    return


def test1(site_no=1, start_time=None):
    if start_time is None:
        start_time = datetime.now().astimezone(timezone.utc)
    back_time = start_time - TEN_YEARS
    time_string = datetime_to_isostring(back_time)
    result1 = influx_client.query("""\
    SELECT "time", site_no, "count", pressure1, internal_temperature, internal_humidity, battery, tube_temperature, tube_humidity, rain, vwc1, vwc2, vwc3, pressure2, external_temperature, external_humidity, flag
    FROM "level1"
    WHERE "time" > '{}' AND site_no='{}';""".format(time_string, site_no))
    points1 = result1.get_points()
    result2 = influx_client.query("""\
    SELECT "time", site_no, "count", pressure1, internal_temperature, internal_humidity, battery, tube_temperature, tube_humidity, rain, vwc1, vwc2, vwc3, pressure2, external_temperature, external_humidity, flag
    FROM "level1_temp"
    WHERE "time" > '{}' AND site_no='{}';""".format(time_string, site_no))
    points2 = result2.get_points()
    _ = next(points1)
    t = 0
    d = 0
    for p1 in points1:
        p2 = next(points2)
        t += 1
        diffkeys = [k for k in p1 if p1[k] != p2[k]]
        for k in diffkeys:
            d += 1
            print(k, ':', p1[k], '->', p2[k])
    return d < 1

def test2(site_no=1, start_time=None):
    if start_time is None:
        start_time = datetime.now().astimezone(timezone.utc)
    back_time = start_time - TEN_YEARS
    time_string = datetime_to_isostring(back_time)
    result1 = influx_client.query("""\
    SELECT "time", site_no, "count", press_corr, wv_corr, intensity_corr, corr_count, flag
    FROM "level2"
    WHERE "time" > '{}' AND site_no='{}';""".format(time_string, site_no))
    points1 = result1.get_points()
    result2 = influx_client.query("""\
    SELECT "time", site_no, "count", press_corr, wv_corr, intensity_corr, corr_count, flag
    FROM "level2_temp"
    WHERE "time" > '{}' AND site_no='{}';""".format(time_string, site_no))
    points2 = result2.get_points()
    t = 0
    d = 0
    for p1 in points1:
        p2 = next(points2)
        t += 1
        diffkeys = [k for k in p1 if p1[k] != p2[k]]
        for k in diffkeys:
            orig_one = p1[k]
            if isinstance(orig_one, float):
                diff = math.fabs(orig_one - p2[k])
                diff_perc = (diff/orig_one) * 100
                #these all seem to be 0.00000088888888 different. (8.8888888e-07)
                if diff_perc < 8.88888912e-07:
                    #print("Close enough:", k, ':', p1[k], '->', p2[k], "d:", diff, "p:", diff_perc)
                    pass
                else:
                    d += 1
                    print("Not close enough:", k, ':', p1[k], '->', p2[k], "d:", diff, "p:", diff_perc)
                    print(p1)
                    print(p2)
            else:
                d += 1
                print("Different!", k, ':', p1[k], '->', p2[k])
                print(p1)
                print(p2)
    if d > 0:
        percent_wrong = (d/t) * 100
        print("Percentage of total entries with differences: {}/{} {}%".format(d, t, percent_wrong))
    return d < 1

def test3(site_no=1, start_time=None):
    if start_time is None:
        start_time = datetime.now().astimezone(timezone.utc)
    back_time = start_time - TEN_YEARS
    time_string = datetime_to_isostring(back_time)
    result1 = influx_client.query("""\
    --SELECT "time", site_no, soil_moist, effective_depth, flag
    SELECT "time", site_no, soil_moist, effective_depth, rainfall, flag
    FROM "level3"
    WHERE "time" > '{}' AND site_no='{}';""".format(time_string, site_no))
    points1 = result1.get_points()
    result2 = influx_client.query("""\
    --SELECT "time", site_no, soil_moist, effective_depth, flag
    SELECT "time", site_no, soil_moist, effective_depth, rainfall, flag
    FROM "level3_temp"
    WHERE "time" > '{}' AND site_no='{}';""".format(time_string, site_no))
    points2 = result2.get_points()
    t = 0
    d = 0
    for p1 in points1:
        p2 = next(points2)
        t += 1
        diffkeys = [k for k in p1 if p1[k] != p2[k]]
        for k in diffkeys:
            orig_one = p1[k]
            if isinstance(orig_one, float):
                diff = math.fabs(orig_one - p2[k])
                diff_perc = (diff / orig_one) * 100
                if diff < 0.00001 or diff_perc < 0.00033:
                    pass
                else:
                    d += 1
                    print("Not close enough:", k, ':', p1[k], '->', p2[k], "d:", diff, "p:", diff_perc)
                    print(p1)
                    print(p2)
            else:
                d += 1
                print("Different!", k, ':', p1[k], '->', p2[k])
                print(p1)
                print(p2)
    if d > 0:
        percent_wrong = (d/t) * 100
        print("Percentage of total entries with differences: {}/{} {}%".format(d, t, percent_wrong))
    return d < 1


def test4(site_no=1, start_time=None):
    if start_time is None:
        start_time = datetime.now().astimezone(timezone.utc)
    back_time = start_time - TEN_YEARS
    time_string = datetime_to_isostring(back_time)
    result1 = influx_client.query("""\
    SELECT "time", site_no, soil_moist, effective_depth, rainfall, soil_moist_filtered, depth_filtered
    FROM "level4"
    WHERE "time" > '{}' AND site_no='{}';""".format(time_string, site_no))
    points1 = result1.get_points()
    result2 = influx_client.query("""\
    SELECT "time", site_no, soil_moist, effective_depth, rainfall, soil_moist_filtered, depth_filtered
    FROM "level4_temp"
    WHERE "time" > '{}' AND site_no='{}';""".format(time_string, site_no))
    points2 = result2.get_points()
    t = 0
    d = 0
    for p1 in points1:
        try:
            p2 = next(points2)
        except StopIteration:
            break
        t += 1
        diffkeys = [k for k in p1 if p1[k] != p2[k]]
        for k in diffkeys:
            orig_one = p1[k]
            if isinstance(orig_one, float):
                diff = math.fabs(orig_one - p2[k])
                diff_perc = (diff / orig_one) * 100
                # these all seem to be 0.00000088888888 different. (8.8888888e-07)
                if diff < 3.29e-05 or diff_perc < 4.8e-06:
                    # print("Close enough: ", k, ':', p1[k], '->', p2[k], "d:", diff)
                    pass
                else:
                    d += 1
                    print("Not close enough:", k, ':', p1[k], '->', p2[k], "d:", diff, "p:", diff_perc)
                    print(p1)
                    print(p2)
            else:
                d += 1
                print("Different!", k, ':', p1[k], '->', p2[k])
                print(p1)
                print(p2)
    if d > 0:
        percent_wrong = (d/t) * 100
        print("Percentage of total entries with differences: {}/{} {}%".format(d, t, percent_wrong))
    return d < 1


def process_levels(site_no, options={}):
    start_time = options.get('start_time', None)
    backprocess = options.get('backprocess', None)
    do_tests = options.get('do_tests', False)
    drop_old = options.get('drop_old', False)
    mongo_client2 = MongoClient(mongodb_config['DB_HOST'], int(mongodb_config['DB_PORT']))  # 27017
    p_start_time = datetime.now().astimezone(timezone.utc)
    if start_time is None:
        start_time = p_start_time
    print("Starting process_levels for site {}, at {}".format(site_no, p_start_time))
    if do_tests:
        print("Doing site {} with sanity tests turned on. This takes longer.".format(site_no))
    #fix_raws(site_no=site_no)
    raw_to_level1(site_no=site_no, start_time=start_time, backprocess=backprocess, drop_old=drop_old)
    print("Finished raw->level1 for site {}, starting level1->level2.".format(site_no))
    if do_tests:
        assert test1(site_no=site_no, start_time=start_time)
    level1_to_level2(mongo_client2, site_no=site_no, start_time=start_time, backprocess=backprocess, drop_old=drop_old)
    print("Finished level1->level2 for site {}, starting level2->level3.".format(site_no))
    if do_tests:
        assert test2(site_no=site_no, start_time=start_time)
    level2_to_level3(mongo_client2, site_no=site_no, start_time=start_time, backprocess=backprocess, drop_old=drop_old)
    print("Finished level2->level3 for site {}, starting level3->level4.".format(site_no))
    if do_tests:
        assert test3(site_no=site_no, start_time=start_time)
    level3_to_level4(site_no=site_no, start_time=start_time, backprocess=backprocess, drop_old=drop_old)
    if do_tests:
        assert test4(site_no=site_no, start_time=start_time)
    p_end_time = datetime.now().astimezone(timezone.utc)
    print("Finished process_levels for site {}, at {}".format(site_no, p_end_time))
    print("Site {} process_levels took {}".format(site_no, (p_end_time-p_start_time)))


# if __name__ == "__main__":
#     from threading import Thread
#     #process_levels(site_no=2, do_tests=True)
#     mdb = getattr(mongo_client, mongodb_config['DB_NAME'])
#     all_sites = mdb.all_sites
#     all_stations_docs = mdb.all_stations
#     all_stations = all_stations_docs.find({}, {'site_no': 1})
#     start_time = datetime.now().astimezone(timezone.utc)
#     threads = []
#     print("Using multithreading")
#     for s in all_stations:
#         site_no = s['site_no']
#         #process_levels(site_no, start_time=start_time, do_tests=False, backprocess=ONE_YEAR)
#         t = Thread(target=process_levels, args=(site_no,), kwargs={'start_time':start_time, 'do_tests': False, 'backprocess': ONE_YEAR})
#         t.start()
#         threads.append(t)
#     _ = [t.join() for t in threads]
#     end_time = datetime.utcnow()
#     print("Finished process_levels for All Sites at {}".format(end_time))
#     print("All sites process_levels took {}".format((end_time-start_time)))

def main():
    start_time = datetime.now().astimezone(timezone.utc)
    parser = argparse.ArgumentParser(description='Run the processing levels on the cosmoz influxdb.')

    parser.add_argument('-s', '--site-number', type=str, dest="siteno",
                        help='Pick just one site number')
    parser.add_argument('-d', '--process-days', type=str, dest="processdays",
                        help='Number of days to backprocess. Default is 365 days.')
    parser.add_argument('-xx', '--dropold', dest="drop_old", action="store_true",
                        help='Drop old contents of table before processing its contents. USE WITH CAUTION!')
    parser.add_argument('-t', '--from-datetime', type=str, dest="fromdatetime",
                        help='The earliest datetime to backprocess to. In isoformat. Default is all of history.\nNote cannot use -d and -t together.')
    parser.add_argument('-o', '--output', dest='output', nargs='?', type=argparse.FileType('w'),
                        help='Send output to a file (defaults to stdout).',
                        default=sys.stdout)
    args = parser.parse_args()
    outfile = args.output
    def printout(*values, sep=' ', end='\n'):
        return print(*values, sep=sep, end=end, file=outfile, flush=True)
    try:
        processdays = args.processdays
        fromdatetime = args.fromdatetime
        drop_old = args.drop_old
        siteno = args.siteno
        if processdays is not None and fromdatetime is not None:
            raise RuntimeError("Cannot use -d and -t at the same time. Pick one.")
        if processdays:
            try:
                processdays = int(processdays)
            except:
                raise RuntimeError("-d must be an integer")
            backprocess = timedelta(days=processdays)
        else:
            if fromdatetime is None:
                backprocess = ONE_YEAR
            else:
                fromdatetime = isostring_to_datetime(fromdatetime)
                backprocess = start_time - fromdatetime
        if backprocess.days < 0:
            raise RuntimeError("Cannot backprocess negative time. Ensure it is positive.")
        mongo_client = MongoClient(mongodb_config['DB_HOST'], int(mongodb_config['DB_PORT']))  # 27017
        mdb = getattr(mongo_client, mongodb_config['DB_NAME'])
        all_sites = mdb.all_sites
        all_stations_docs = mdb.all_stations
        if siteno is not None:
            sitenos = [int(s.strip()) for s in siteno.split(',') if s]
            all_stations = all_stations_docs.find({'site_no': {"$in": sitenos}}, {'site_no': 1})
        else:
            all_stations = all_stations_docs.find({}, {'site_no': 1})
        mongo_client.close()
        worker_options = {'start_time': start_time, 'do_tests': False, 'backprocess': backprocess, 'drop_old': drop_old}
        all_stations = list(all_stations) #This turns a the mongo cursor into a python list
        if len(all_stations) < 1:
            printout("No stations to process.")
            return
        elif len(all_stations) < 2:
            printout("Only doing station {}".format(siteno))
            process_levels(all_stations[0]['site_no'], worker_options)
            end_time = datetime.now().astimezone(timezone.utc)
            printout("Finished process_levels for site {} at {}".format(siteno, end_time))
            printout("process_levels took {}".format((end_time - start_time)))
        else:
            printout("Using multiprocessing")
            processes = []
            pool = Pool(None)  # uses os.cpu_count
            with pool as p:
                worker_args = [(s['site_no'], worker_options) for s in all_stations]
                p.starmap(process_levels, worker_args)
            end_time = datetime.now().astimezone(timezone.utc)
            printout("Finished process_levels for All Sites at {}".format(end_time))
            printout("All sites process_levels took {}".format((end_time - start_time)))
    finally:
        outfile.close()

if __name__ == "__main__":
    main()
