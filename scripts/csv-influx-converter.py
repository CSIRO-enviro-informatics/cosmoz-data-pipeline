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
import csv
from influxdb import InfluxDBClient
from influx_cached_writer import AccumCacheInfluxWriter
from _influx_db_config import consts as influx_config
from utils import sql_to_isostring

influx_client = InfluxDBClient(
    influx_config['DB_HOST'], influx_config['DB_PORT'],
    influx_config['DB_USERNAME'], influx_config['DB_PASSWORD'],
    influx_config['DB_NAME'], timeout=30)

def intensities():
    with open("./intensity.csv", "r", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="\t")
        headers = next(r)
        with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
            while True:
                try:
                    row = next(r)
                except (IndexError, StopIteration):
                    break
                print(row)
                row_dict = {}
                site_num = int(row[0])
                for ci, cval in enumerate(row):
                    col_name = headers[ci]
                    row_dict[col_name] = cval
                iso_timestamp = sql_to_isostring(row_dict['Timestamp'])
                bad_data = int(row_dict['BadDataFlag'])
                try:
                    intensity = float(row_dict['Intensity'])
                except ValueError:
                    intensity = 0.0
                    bad_data = 1
                json_body = {
                        "measurement": "intensity",
                        "tags": {
                            "site_no": site_num,
                            "bad_data_flag": bad_data,
                        },
                        "time": iso_timestamp, #"2009-11-10T23:00:00Z",
                        "fields": {
                            "intensity": intensity
                        }
                    }
                writer.write_point(json_body)
                del row


def silo_data():
    with open("./silo.csv", "r", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="\t")
        headers = next(r)
        with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
            while True:
                try:
                    row = next(r)
                except (IndexError, StopIteration):
                    break
                print(row)
                row_dict = {}
                site_num = int(row[0])
                for ci, cval in enumerate(row):
                    col_name = headers[ci]
                    row_dict[col_name] = cval
                iso_timestamp = sql_to_isostring(row_dict['Date2'])
                json_body = {
                        "measurement": "silo_data",
                        "tags": {
                            "site_no": site_num,
                        },
                        "time": iso_timestamp, #"2009-11-10T23:00:00Z",
                        "fields": {
                            "t_max": float(row_dict['T_Max']),
                            "smx": float(row_dict['Smx']),
                            "t_min": float(row_dict['T_Min']),
                            "smn": float(row_dict['Smn']),
                            "rain": float(row_dict['Rain']),
                            "srn": float(row_dict['Srn']),
                            "evap": float(row_dict['Evap']),
                            "sev": float(row_dict['Sev']),
                            "radn": float(row_dict['Radn']),
                            "ssl": float(row_dict['Ssl']),
                            "vp": float(row_dict['VP']),
                            "svp": float(row_dict['Svp']),
                            "rh_max_t": float(row_dict['RHmaxT']),
                            "rh_min_t": float(row_dict['RHminT']),
                            "average_temperature": float(row_dict['AverageTemperature']),
                            "average_humidity": float(row_dict['AverageHumidity'])
                        }
                    }
                writer.write_point(json_body)


def level1():
    with open("./level1.csv", "r", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="\t")
        headers = next(r)
        with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
            while True:
                try:
                    row = next(r)
                except (IndexError, StopIteration):
                    break
                print(row)
                row_dict = {}
                site_num = int(row[0])
                for ci, cval in enumerate(row):
                    col_name = headers[ci]
                    row_dict[col_name] = cval
                iso_timestamp = sql_to_isostring(row_dict['Timestamp'])
                json_body = {
                        "measurement": "level1",
                        "tags": {
                            "site_no": site_num,
                            "flag": int(row_dict['Flag']),
                        },
                        "time": iso_timestamp, #"2009-11-10T23:00:00Z",
                        "fields": {
                            "count": int(row_dict['Count']),
                            "pressure1": float(row_dict['Pressure1']),
                            "internal_temperature": float(row_dict['InternalTemperature']),
                            "internal_humidity": float(row_dict['InternalHumidity']),
                            "battery": float(row_dict['Battery']),
                            "tube_temperature": float(row_dict['TubeTemperature']),
                            "tube_humidity": float(row_dict['TubeHumidity']),
                            "rain": float(row_dict['Rain']),
                            "vwc1": float(row_dict['VWC1']),
                            "vwc2": float(row_dict['VWC2']),
                            "vwc3": float(row_dict['VWC3']),
                            "pressure2": float(row_dict['Pressure2']),
                            "external_temperature": float(row_dict['ExternalTemperature']),
                            "external_humidity": float(row_dict['ExternalHumidity']),
                        }
                    }
                writer.write_point(json_body)


def level2():
    with open("./level2.csv", "r", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="\t")
        headers = next(r)
        with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
            while True:
                try:
                    row = next(r)
                except (IndexError, StopIteration):
                    break
                print(row)
                row_dict = {}
                site_num = int(row[0])
                for ci, cval in enumerate(row):
                    col_name = headers[ci]
                    row_dict[col_name] = cval
                iso_timestamp = sql_to_isostring(row_dict['Timestamp'])
                json_body = {
                        "measurement": "level2",
                        "tags": {
                            "site_no": site_num,
                            "flag": int(row_dict['Flag']),
                        },
                        "time": iso_timestamp, #"2009-11-10T23:00:00Z",
                        "fields": {
                            "count": int(row_dict['Count']),
                            "press_corr": float(row_dict['PressCorr']),
                            "wv_corr": float(row_dict['WVCorr']),
                            "intensity_corr": float(row_dict['IntensityCorr']),
                            "corr_count": float(row_dict['CorrCount'])
                        }
                    }
                writer.write_point(json_body)


def level3():
    with open("./level3.csv", "r", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="\t")
        headers = next(r)
        with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
            while True:
                try:
                    row = next(r)
                except (IndexError, StopIteration):
                    break
                print(row)
                row_dict = {}
                site_num = int(row[0])
                for ci, cval in enumerate(row):
                    col_name = headers[ci]
                    row_dict[col_name] = cval
                iso_timestamp = sql_to_isostring(row_dict['Timestamp'])
                json_body = {
                        "measurement": "level3",
                        "tags": {
                            "site_no": site_num,
                            "flag": int(row_dict['Flag']),
                        },
                        "time": iso_timestamp, #"2009-11-10T23:00:00Z",
                        "fields": {
                            "soil_moist": float(row_dict['SoilMoist']),
                            "effective_depth": float(row_dict['EffectiveDepth']),
                            "rainfall": float(row_dict['Rainfall'])
                        }
                    }
                writer.write_point(json_body)


def level4():
    influx_client.drop_measurement("level4")
    with open("./level4.csv", "r", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="\t")
        headers = next(r)
        with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
            while True:
                try:
                    row = next(r)
                except (IndexError, StopIteration):
                    break
                print(row)
                row_dict = {}
                site_num = int(row[0])
                for ci, cval in enumerate(row):
                    col_name = headers[ci]
                    row_dict[col_name] = cval
                iso_timestamp = sql_to_isostring(row_dict['Timestamp'])
                try:
                    soil_moist_filtered = float(row_dict['SoilMoistFiltered'])
                except ValueError:
                    soil_moist_filtered = 0.0
                try:
                    depth_filtered = float(row_dict['DepthFiltered'])
                except ValueError:
                    depth_filtered = 0.0
                json_body = {
                        "measurement": "level4",
                        "tags": {
                            "site_no": site_num
                        },
                        "time": iso_timestamp, #"2009-11-10T23:00:00Z",
                        "fields": {
                            "soil_moist": float(row_dict['SoilMoist']),
                            "effective_depth": float(row_dict['EffectiveDepth']),
                            "rainfall": float(row_dict['Rainfall']),
                            "soil_moist_filtered": soil_moist_filtered,
                            "depth_filtered": depth_filtered
                        }
                    }
                writer.write_point(json_body)


def raw_vals():
    with open("./raw.csv", "r", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="\t")
        headers = next(r)
        with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
            while True:
                try:
                    row = next(r)
                except (IndexError, StopIteration):
                    break
                print(row)
                row_dict = {}
                site_num = int(row[0])
                for ci, cval in enumerate(row):
                    col_name = headers[ci]
                    row_dict[col_name] = cval
                iso_timestamp = sql_to_isostring(row_dict['Timestamp'])
                json_body = {
                        "measurement": "raw_values",
                        "tags": {
                            "site_no": site_num,
                            "flag": int(row_dict['Flag']),
                        },
                        "time": iso_timestamp, #"2009-11-10T23:00:00Z",
                        "fields": {
                            "count": int(row_dict['Count']),
                            "pressure1": float(row_dict['Pressure1']),
                            "internal_temperature": float(row_dict['InternalTemperature']),
                            "internal_humidity": float(row_dict['InternalHumidity']),
                            "battery": float(row_dict['Battery']),
                            "tube_temperature": float(row_dict['TubeTemperature']),
                            "tube_humidity": float(row_dict['TubeHumidity']),
                            "rain": float(row_dict['Rain']),
                            "vwc1": float(row_dict['VWC1']),
                            "vwc2": float(row_dict['VWC2']),
                            "vwc3": float(row_dict['VWC3']),
                            "pressure2": float(row_dict['Pressure2']),
                            "external_temperature": float(row_dict['ExternalTemperature']),
                            "external_humidity": float(row_dict['ExternalHumidity']),
                        }
                    }
                writer.write_point(json_body)

if __name__ == "__main__":
    influx_client.create_database(influx_config['DB_NAME'])
    silo_data()
    intensities()
    # raw_vals()
    # level2()
    # level1()
    #level3()
    #level4()
