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


def check_in_raw(site_no=0, start_time=None, processdays=None, outfile=None):
    result = influx_client.query("""SELECT * FROM "raw_values" WHERE site_no=$s;""", bind_params={'s': str(site_no)})
    points = result.get_points()
    print("Loading all points in raw table...")
    all_records = {}  # this really should be a pandas dataframe.. but doesn't matter..
    count = 0
    for p in points:
        timestr = p.pop('time')
        this_datetime = isostring_to_datetime(timestr)
        if this_datetime in all_records:
            raise Exception("Got unexpected temporal duplicate: site {} time {}".format(site_no, timestr))
        count += 1

        all_records[this_datetime] = p
        if count % 100 == 0:
            print("Reading {}...           \r".format(count), end="", flush=True)
    print("Read {} records!".format(count))
    all_times = SortedList(all_records.keys())
    maybe_duplicates = {}
    print("Checking for duplicates.")
    for i, (at_time, params) in enumerate(all_records.items()):
        if i % 100 == 0:
            percentage = (i/count) * 100
            print("Completed {}% - {}       \r".format(percentage, at_time), end="", flush=True)
        # check up to 30 mins before this time
        backtime = at_time - timedelta(minutes=29.0)
        possible_duplicates_times = list(all_times.irange(backtime, at_time, inclusive=(True,False)))
        if len(possible_duplicates_times) < 1:
            continue
        (battery1, count1, tube_temperature1, rain1) = (params['battery'], params['count'], params['tube_temperature'], params['rain'])
        pressure_ref = params['pressure1']
        use_pressure = 'pressure1'
        pressure2_ref = params['pressure2']
        if (pressure2_ref is not None and pressure2_ref > 10) and (pressure_ref is None or pressure_ref < 10):
            pressure_ref = params['pressure2']
            use_pressure = 'pressure2'
        maybe_duplicates[at_time] = list()
        for p in possible_duplicates_times:
            rec = all_records[p]
            (battery2, count2, tube_temperature2, rain2) = (rec['battery'], rec['count'], rec['tube_temperature'], rec['rain'])
            pressure_ref2 = rec[use_pressure]
            matches = 0
            for (param1, param2) in ((battery1,battery2),(count1,count2),(tube_temperature1,tube_temperature2),(rain1,rain2), (pressure_ref,pressure_ref2)):
                if param1 == param2:
                    matches += 1
            if matches >= 5:
                maybe_duplicates[at_time].append((p, rec))
    if maybe_duplicates:
        if outfile is None:
            outfile = "./duplicates_2_s{}.txt".format(site_no)
        with open(outfile, "w") as f:
            f.write("Results of search for duplicates at station number {}\n".format(site_no))
            for (di, dv) in maybe_duplicates.items():
                dvlen = len(dv)
                if dvlen < 1:
                    continue
                if dvlen >1:
                    print("Got more than one!")
                f.write("\nTime {} is potentially a duplicate of {} previous records:\n".format(di, dvlen))
                rec = all_records[di]
                f.write("\tThis record: {}\n".format(rec))
                for i,(dvt,dvv) in enumerate(dv):
                    before_time = "minutes"
                    offset = (di-dvt).total_seconds()
                    mins_before = (offset / 60.0)
                    if mins_before < 1.0:
                        mins_before = offset
                        before_time = "seconds"
                    f.write("\tRecord at {} {} before:\n\t{} - {}\n".format(mins_before, before_time, dvt.time(), dvv))
    print("Finished checking duplicates in raw for station {}".format(site_no))
    return

def impl_detect_duplicates(site_no, options=None):
    if options is None:
        options = {}
    start_time = options.get('start_time', None)
    processdays = options.get('processdays', None)
    delete_them = options.get('delete_them', False)
    mongo_client2 = MongoClient(mongodb_config['DB_HOST'], int(mongodb_config['DB_PORT']))  # 27017
    p_start_time = datetime.now().astimezone(timezone.utc)
    if start_time is None:
        start_time = p_start_time
    print("Starting detect_duplicates for site {}, at {}".format(site_no, p_start_time))
    #fix_raws(site_no=site_no)
    check_in_raw(site_no=site_no, start_time=start_time, processdays=processdays)
    p_end_time = datetime.now().astimezone(timezone.utc)
    print("Finished process_levels for site {}, at {}".format(site_no, p_end_time))
    print("Site {} process_levels took {}".format(site_no, (p_end_time-p_start_time)))



def main():
    start_time = datetime.now().astimezone(timezone.utc)
    parser = argparse.ArgumentParser(description='Find duplicates, remove them if instructed.')

    parser.add_argument('-s', '--site-number', type=str, dest="siteno",
                        help='Pick just one site number')
    parser.add_argument('-d', '--process-days', type=str, dest="processdays",
                        help='Number of days to backprocess. Default is 365 days.')
    parser.add_argument('-x', '--delete', dest="delete", action="store_true",
                        help='Delete them. Default is false.')
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
        delete_them = args.delete
        siteno = args.siteno
        if processdays is not None and fromdatetime is not None:
            raise RuntimeError("Cannot use -d and -t at the same time. Pick one.")
        if processdays:
            try:
                processdays = int(processdays)
            except:
                raise RuntimeError("-d must be an integer")
            processdays = timedelta(days=processdays)
        else:
            if fromdatetime is None:
                processdays = ONE_YEAR
            else:
                fromdatetime = isostring_to_datetime(fromdatetime)
                processdays = start_time - fromdatetime
        if processdays.days < 0:
            raise RuntimeError("Cannot process negative time. Ensure it is positive.")
        mongo_client = MongoClient(mongodb_config['DB_HOST'], int(mongodb_config['DB_PORT']))  # 27017
        mdb = getattr(mongo_client, mongodb_config['DB_NAME'])
        all_sites = mdb.all_sites
        all_stations_docs = mdb.all_stations
        if siteno is not None:
            sitenos = [int(s.strip()) for s in siteno.split(',') if s]
            all_stations = all_stations_docs.find({'site_no': {"$in": sitenos}}, {'site_no': 1})
        else:
            all_stations = all_stations_docs.find({}, {'site_no': 1})
        all_stations = list(all_stations)  # This turns a the mongo cursor into a python list
        mongo_client.close()
        worker_options = {'start_time': start_time, 'delete_them': delete_them, 'processdays': processdays}
        if len(all_stations) < 1:
            printout("No stations to check.")
            return
        elif len(all_stations) < 2:
            printout("Only doing station {}".format(siteno))
            impl_detect_duplicates(all_stations[0]['site_no'], worker_options)
            end_time = datetime.now().astimezone(timezone.utc)
            printout("Finished detect_duplicates for site {} at {}".format(siteno, end_time))
            printout("detect_duplicates took {}".format((end_time - start_time)))
        else:
            printout("Using multiprocessing")
            processes = []
            pool = Pool(None)  # uses os.cpu_count
            with pool as p:
                worker_args = [(s['site_no'], worker_options) for s in all_stations]
                p.starmap(impl_detect_duplicates, worker_args)
            end_time = datetime.now().astimezone(timezone.utc)
            printout("Finished detect_duplicates for All Sites at {}".format(end_time))
            printout("All sites detect_duplicates took {}".format((end_time - start_time)))
    finally:
        outfile.close()

if __name__ == "__main__":
    main()
