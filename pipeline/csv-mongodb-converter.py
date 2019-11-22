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
from io import StringIO
import csv
import time
import datetime
from urllib import request
from bson import Decimal128
from pymongo import MongoClient
from ._mongo_db_config import config as mongodb_config
from .utils import isostring_to_datetime, sql_to_isostring

client = MongoClient(mongodb_config['DB_HOST'], int(mongodb_config['DB_PORT']))  # 27017

def all_stations():
    db = getattr(client, mongodb_config['DB_NAME'])
    all_stations_collection = db.all_stations
    with open("./all_stations.tsv", "r", encoding="latin1") as f:
        r = csv.reader(f, delimiter="\t")
        headers = next(r)
        accum_cache = []
        while True:
            try:
                row = next(r)
            except (IndexError, StopIteration):
                break
            print(row)
            row_dict = {}
            for ci, cval in enumerate(row):
                col_name = headers[ci]
                row_dict[col_name] = cval
            doc_body = {
                'site_no': row_dict['site_no'],
                'site_name': row_dict['site_name'],
                'tube_type': row_dict['tube_type'],
                'network': row_dict['network'],
                'imei': row_dict['imei'],
                'sat_data_select': row_dict['sat_data_select'],
                'hydroinnova_serial_no': row_dict['hydroinnova_serial_no'],
                'latitude': Decimal128(row_dict['latitude']),
                'longitude': Decimal128(row_dict['longitude']),
                'altitude': Decimal128(row_dict['altitude']),
                'installation_date': isostring_to_datetime(row_dict['installation_date']),
                'contact': row_dict['contact'],
                'email': row_dict['email'],
                'site_description': row_dict['site_description'],
                'calibration_type': row_dict['calibration_type'],
                'timezone': str(row_dict['timezone']),
                'ref_pressure': Decimal128(row_dict['ref_pressure']),
                'ref_intensity': Decimal128(row_dict['ref_intensity']),
                'cutoff_rigidity': Decimal128(row_dict['cutoff_rigidity']),
                'elev_scaling': Decimal128(row_dict['elev_scaling']),
                'latit_scaling': Decimal128(row_dict['latit_scaling']),
                'scaling': Decimal128(row_dict['scaling']),
                'beta': Decimal128(row_dict['beta']),
                'n0_cal': Decimal128(row_dict['n0_cal']),
                'bulk_density': Decimal128(row_dict['bulk_density']),
                'lattice_water_g_g': Decimal128(row_dict['lattice_water_g_g']),
                'soil_organic_matter_g_g': Decimal128(row_dict['soil_organic_matter_g_g']),
                'site_photo_name': row_dict['site_photo_name'],
                'nmdb': row_dict['nmdb']
            }
            accum_cache.append(doc_body)
            if len(accum_cache) > 9:
                try:
                    all_stations_collection.insert_many(accum_cache)
                except TimeoutError as er:
                    msg = er.args[0]
                    if b'"timeout"' in msg:
                        time.sleep(5)
                        all_stations_collection.insert_many(accum_cache)
                    else:
                        raise er
                accum_cache = []
            del row
        if len(accum_cache):
            try:
                all_stations_collection.insert_many(accum_cache)
            except TimeoutError as er:
                msg = er.args[0]
                if b'"timeout"' in msg:
                    time.sleep(5)
                    all_stations_collection.insert_many(accum_cache)
                else:
                    raise er
            del accum_cache

def stations_calibration():
    db = getattr(client, mongodb_config['DB_NAME'])
    all_stations_collection = db.all_stations
    cal_txt_collection = db.stations_calibration_txt
    cal_collection = db.stations_calibration
    res = all_stations_collection.find({})
    for i in res:
        time.sleep(0.5)
        site_no = i['site_no']
        url = "http://cosmoz.csiro.au/sensor-information/?SiteNo={:0>2}".format(site_no)
        r = request.Request(url)
        try:
            with request.urlopen(r) as f:
                pass
        except Exception as e:
            pass
        time.sleep(0.5)
        url = "http://cosmoz.csiro.au/wp-content/themes/cosmoz/data-files/Site{:0>2}_SCD.txt".format(site_no)
        r = request.Request(url)
        try:
            with request.urlopen(r) as f:
                resp = f.read()
                if resp:
                    resp = resp.decode("utf-8")
        except Exception as e:
            print(e)
            raise
        if not resp:
            continue
        # doc_body = {
        #     "site_no": site_no,
        #     "calibration_txt": resp
        # }
        # cal_txt_collection.update_one({"site_no": site_no}, {"$set": doc_body}, upsert=True)
        stringio = StringIO(resp)
        lines = iter(stringio.readlines())
        h1 = next(lines)
        h2 = next(lines)
        headers = ["date", "label", "loc", "depth", "vol", "total_wet", "total_dry", "tare", "soil_wet", "soil_dry", "gwc", "bd", "vwc"]
        to_add = []
        try:
            line = next(lines)
        except:
            line = None
        while line:
            line = line.replace("                 ", "  NULL  NULL  ")
            line = line.replace("         ", "  NULL  ")

            parts = line.split(" ")
            parts = list(filter(None,[p.strip(" \n\r\t") for p in parts]))
            if parts[3] == "NULL":
                _ = parts.pop(3)
            if parts[2] == "NULL":
                _ = parts.pop(2)
            for pi,part in enumerate(parts):
                if part == "to":
                    prev = parts.pop(pi-1)
                    after = parts.pop(pi)
                    if len(prev) > 2:
                        prevprev = prev[0:-2]
                        prev = prev[-2:]
                        parts.insert(pi-1, prevprev)
                        pi+=1
                    parts[pi-1] = "{} to {}".format(prev, after)
                    if parts[pi] == "cm":
                        after_after = parts.pop(pi)
                        parts[pi-1] = "{} {}".format(parts[pi-1], after_after)
                    break
                if part == "cm":
                    prev = parts.pop(pi-1)
                    parts[pi-1] = "{} cm".format(prev)
                    break
            try:
                assert len(parts) == len(headers)
                tagged_parts = {headers[i]: p for i, p in enumerate(parts)}
                tagged_parts['date'] = datetime.datetime.strptime(tagged_parts['date'], "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
                for kl in ("total_wet", "total_dry", "tare", "soil_wet", "soil_dry", "gwc", "bd", "vwc"):
                    part = tagged_parts[kl]
                    tagged_parts[kl] = Decimal128('NaN') if (part == "" or part == "NULL") else Decimal128(part)
                tagged_parts['site_no'] = site_no
                to_add.append(tagged_parts)
            except Exception as e:
                print(e)
                print(line)
                continue
            finally:
                try:
                    line = next(lines)
                except:
                    line = None
        if to_add:
            cal_collection.insert_many(to_add)

if __name__ == "__main__":
    all_stations()
    stations_calibration()
