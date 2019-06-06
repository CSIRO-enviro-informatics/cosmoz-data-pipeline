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
from datetime import datetime, timedelta, date, timezone
from sql_db import CosmozSQLConnection
from influxdb import InfluxDBClient
from influx_cached_writer import AccumCacheInfluxWriter
from _influx_db_config import consts as influx_config
from utils import datetime_to_sql, isostring_to_datetime, datetime_to_isostring, sql_to_isostring

influx_client = InfluxDBClient(
    influx_config['DB_HOST'], influx_config['DB_PORT'],
    influx_config['DB_USERNAME'], influx_config['DB_PASSWORD'],
    influx_config['DB_NAME'], timeout=30)


def intensities(from_time=None):
    if from_time is None:
        from_time = datetime.min
    #from_time = datetime.now().astimezone(timezone.utc) - timedelta(days=120)
    from_time_sql = datetime_to_sql(from_time)
    con1 = CosmozSQLConnection()
    with con1 as conn:
        with conn.cursor(as_dict=True) as cur:
            sql = "SELECT * FROM dbo.Intensity WHERE Timestamp >= %s;"
            cur.execute(sql, from_time_sql)
            with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
                while True:
                    try:
                        row = cur.fetchone()
                    except (IndexError, StopIteration):
                        break
                    if row is None or row is False:
                        break
                    site_num = int(row['SiteNo'])
                    iso_timestamp = datetime_to_isostring(row['Timestamp'])
                    bad_data = int(row['BadDataFlag'])
                    try:
                        intensity = float(row['Intensity'])
                    except (TypeError, ValueError):
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


def silo_data(from_time=None):
    if from_time is None:
        from_time = datetime.min
    from_time_sql = datetime_to_sql(from_time)
    con1 = CosmozSQLConnection()
    with con1 as conn:
        with conn.cursor(as_dict=True) as cur:
            sql = "SELECT * FROM dbo.SiloData WHERE Date2 >= %s;"
            cur.execute(sql, from_time_sql)
            with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
                while True:
                    try:
                        row = cur.fetchone()
                    except (IndexError, StopIteration):
                        break
                    if row is None or row is False:
                        break
                    #print(row)
                    site_num = int(row['SiteNo'])
                    iso_timestamp = datetime_to_isostring(row['Date2'])
                    json_body = {
                            "measurement": "silo_data",
                            "tags": {
                                "site_no": site_num,
                            },
                            "time": iso_timestamp, #"2009-11-10T23:00:00Z",
                            "fields": {
                                "t_max": float(row['T_Max']),
                                "smx": float(row['Smx']),
                                "t_min": float(row['T_Min']),
                                "smn": float(row['Smn']),
                                "rain": float(row['Rain']),
                                "srn": float(row['Srn']),
                                "evap": float(row['Evap']),
                                "sev": float(row['Sev']),
                                "radn": float(row['Radn']),
                                "ssl": float(row['Ssl']),
                                "vp": float(row['VP']),
                                "svp": float(row['Svp']),
                                "rh_max_t": float(row['RHmaxT']),
                                "rh_min_t": float(row['RHminT']),
                                "average_temperature": float(row['AverageTemperature']),
                                "average_humidity": float(row['AverageHumidity'])
                            }
                        }
                    writer.write_point(json_body)


def raw_vals(from_time=None):
    if from_time is None:
        from_time = datetime.min
    #from_time = datetime.utcnow() - timedelta(days=120)
    from_time_sql = datetime_to_sql(from_time)
    con1 = CosmozSQLConnection()
    with con1 as conn:
        with conn.cursor(as_dict=True) as cur:
            sql = "SELECT * FROM dbo.RawData WHERE Timestamp >= %s;"
            cur.execute(sql, from_time_sql)
            with AccumCacheInfluxWriter(influx_client, cache_length=10) as writer:
                while True:
                    try:
                        row = cur.fetchone()
                    except (IndexError, StopIteration):
                        break
                    if row is None or row is False:
                        break
                    site_num = int(row['SiteNo'])
                    iso_timestamp = datetime_to_isostring(row['Timestamp'])
                    json_body = {
                            "measurement": "raw_values",
                            "tags": {
                                "site_no": site_num,
                                "flag": int(row['Flag']),
                            },
                            "time": iso_timestamp, #"2009-11-10T23:00:00Z",
                            "fields": {
                                "count": int(row['Count']),
                                "pressure1": float(row['Pressure1']),
                                "internal_temperature": float(row['InternalTemperature']),
                                "internal_humidity": float(row['InternalHumidity']),
                                "battery": float(row['Battery']),
                                "tube_temperature": float(row['TubeTemperature']),
                                "tube_humidity": float(row['TubeHumidity']),
                                "rain": float(row['Rain']),
                                "vwc1": float(row['VWC1']),
                                "vwc2": float(row['VWC2']),
                                "vwc3": float(row['VWC3']),
                                "pressure2": float(row['Pressure2']),
                                "external_temperature": float(row['ExternalTemperature']),
                                "external_humidity": float(row['ExternalHumidity']),
                            }
                        }
                    writer.write_point(json_body)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Pre-populate the cosmoz influx database with values from the mssql db.')

    parser.add_argument('-d', '--process-days', type=str, dest="processdays",
                        help='Number of days to backprocess. Default is all of history.')
    parser.add_argument('-t', '--from-datetime', type=str, dest="fromdatetime",
                        help='The earliest datetime to pre-populate to. In isoformat. Default is all of history.\nNote cannot use -d and -t together.')
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
        if processdays is not None and fromdatetime is not None:
            raise RuntimeError("Cannot use -d and -t at the same time. Pick one.")
        if processdays:
            try:
                processdays = int(processdays)
            except:
                raise RuntimeError("-d must be an integer")
            from_time = datetime.now().astimezone(timezone.utc) - timedelta(days=processdays)
        else:
            if fromdatetime is None:
                from_time = datetime.min
            else:
                from_time = isostring_to_datetime(fromdatetime)
        influx_client.create_database(influx_config['DB_NAME'])
        printout("Importing SILO Data.")
        silo_data(from_time)
        printout("Importing Intensities vals.")
        intensities(from_time)
        printout("Importing raw vals.")
        raw_vals(from_time)
    finally:
        printout("Done.")
        outfile.close()
