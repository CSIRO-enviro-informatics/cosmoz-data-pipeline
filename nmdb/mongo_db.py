# -*- coding: utf-8 -*-
#
from pymongo import MongoClient
from ._mongo_db_config import config as mongodb_config
from datetime import datetime

#mongo_client = MongoClient(mongodb_config['DB_HOST'], int(mongodb_config['DB_PORT']))  # 27017

def make_mongo_client(new=False):
    if not new and make_mongo_client.cached:
        return make_mongo_client.cached
    if new:
        make_mongo_client.cached = None
    mongo_client = MongoClient(mongodb_config['DB_HOST'], int(mongodb_config['DB_PORT']))  # 27017
    make_mongo_client.cached = mongo_client
    return mongo_client
make_mongo_client.cached = None


def get_site_no_from_imei(imei, at_date=None, mongo_client=None):
    if mongo_client is None:
        mongo_client = make_mongo_client()
    mdb = getattr(mongo_client, mongodb_config['DB_NAME'])
    all_stations = mdb.all_stations
    found_sites = all_stations.find({'imei': imei})
    found_sites = [f for f in found_sites]
    if len(found_sites) < 1:
        return 0
    elif len(found_sites) < 2:
        found_site = found_sites[0]
    elif at_date is None:
        # get the one most recently installed
        found_sites.sort(key=lambda x: x.get('installation_date', None), reverse=True)
        found_site = found_sites[0]
    else:
        # get the one most recently installed but before sent_date
        found_sites.sort(key=lambda x: x.get('installation_date', None), reverse=True)
        found_site = found_sites[-1]
        if isinstance(at_date, datetime):
            at_date = at_date.date()
        for f in found_sites:
            i_date = f.get('installation_date', None)
            if i_date is None:
                continue
            if isinstance(i_date, datetime):
                i_date = i_date.date()
            if i_date <= at_date:
                found_site = f
                break
    if not found_site:
        return 0
    try:
        site_no = found_site["site_no"]
    except LookupError:
        site_no = 0
    return site_no


def get_all_site_no(mongo_client=None):
    if mongo_client is None:
        mongo_client = make_mongo_client()
    mdb = getattr(mongo_client, mongodb_config['DB_NAME'])
    all_stations = mdb.all_stations
    found_sites = all_stations.find()
    all_sites = []
    for f in found_sites:
        all_sites.append(f['site_no'])
    return all_sites

def get_nmdb_from_site_no(site_no, mongo_client=None):
    if mongo_client is None:
        mongo_client = make_mongo_client()
    mdb = getattr(mongo_client, mongodb_config['DB_NAME'])
    all_stations = mdb.all_stations
    found_site = all_stations.find_one({'site_no': site_no})
    if not found_site:
        return 0
    try:
        nmdb = found_site["nmdb"]
    except LookupError:
        nmdb = 0
    return nmdb
