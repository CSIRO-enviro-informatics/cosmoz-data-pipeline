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
import time
from datetime import datetime
from bson import Decimal128
from pymongo import MongoClient

client = MongoClient('localhost', 27018)  # 27017

def isostring_to_datetime(iso_string):
    try:
       return datetime.strptime(iso_string, "%Y-%m-%dT%H:%M:%SZ")
    except:
       return datetime.strptime(iso_string, "%Y-%m-%dT%H:%M:%S.%fZ")

def sql_to_isostring(sql_datetime):
    timestamp_parts = str(sql_datetime).split(' ')
    return "{}T{}Z".format(timestamp_parts[0], timestamp_parts[1])

def all_stations():
    db = client.cosmoz
    all_stations_collection = db.all_stations
    with open("./stations.csv", "r", encoding="latin1") as f:
        r = csv.reader(f, delimiter="\t")
        headers = next(r)
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
            doc_body = {
                'site_no': site_num,
                'site_name': row_dict['SiteName'],
                'tube_type': row_dict['TubeType'],
                'network': row_dict['Network'],
                'imei': row_dict['IMEI'],
                'sat_data_select': row_dict['SatDataSelect'],
                'hydroinnova_serial_no': row_dict['HydroinnovaSerialNo'],
                'latitude': Decimal128(row_dict['Latitude']),
                'longitude': Decimal128(row_dict['Longitude']),
                'altitude': Decimal128(row_dict['Altitude']),
                'installation_date': isostring_to_datetime(sql_to_isostring(row_dict['InstallationDate'])),
                'contact': row_dict['Contact'],
                'email': row_dict['Email'],
                'site_description': row_dict['SiteDescription'],
                'calibration_type': row_dict['CalibrationType'],
                'timezone': str(row_dict['Timezone']),
                'ref_pressure': Decimal128(row_dict['RefPressure']),
                'ref_intensity': Decimal128(row_dict['RefIntensity']),
                'cutoff_rigidity': Decimal128(row_dict['CutoffRigidity']),
                'elev_scaling': Decimal128(row_dict['ElevScaling']),
                'latit_scaling': Decimal128(row_dict['LatitScaling']),
                'scaling': Decimal128(row_dict['Scaling']),
                'beta': Decimal128(row_dict['Beta']),
                'n0_cal': Decimal128(row_dict['N0_Cal']),
                'bulk_density': Decimal128(row_dict['BulkDensity']),
                'lattice_water_g_g': Decimal128(row_dict['LatticeWater_g_g']),
                'soil_organic_matter_g_g': Decimal128(row_dict['SoilOrganicMatter_g_g']),
                'site_photo_name': row_dict['SitePhotoName'],
                'nmdb': row_dict['NMDB']
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


if __name__ == "__main__":
    all_stations()
