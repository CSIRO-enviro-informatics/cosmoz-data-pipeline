# -*- coding: utf-8 -*-
#
from pymongo import MongoClient
from ._mongo_db_config import config as mongodb_config
from .utils import isostring_to_datetime, sql_to_isostring

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


def get_site_no_from_imei(imei, mongo_client=None):
    if mongo_client is None:
        mongo_client = make_mongo_client()
    mdb = getattr(mongo_client, mongodb_config['DB_NAME'])
    all_stations = mdb.all_stations
    found_site = all_stations.find_one({'imei': imei})
    if not found_site:
        return 0
    try:
        site_no = found_site["site_no"]
    except LookupError:
        site_no = 0
    return site_no


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
