# -*- coding: utf-8 -*-
#
import sys
from os import getenv
try:
    from .utils import do_load_dotenv
except ImportError:
    from utils import do_load_dotenv

do_load_dotenv()

module = sys.modules[__name__]
defaults = module.defaults = {
    "DB_HOST": "timeseriesdb",
    "DB_PORT": 8086,
    "DB_NAME": "cosmoz",
    "DB_USERNAME": "root",
    "DB_PASSWORD": "root",
}
config = module.config = dict()

config['DB_HOST'] = getenv("INFLUX_DB_HOST", None)
config['DB_PORT'] = getenv("INFLUX_DB_PORT", None)
config['DB_NAME'] = getenv("INFLUX_DB_NAME", None)
config['DB_USERNAME'] = getenv("INFLUX_DB_USERNAME", None)
config['DB_PASSWORD'] = getenv("INFLUX_DB_PASSWORD", None)

for k, v in defaults.items():
    if k not in config or config[k] is None:
        config[k] = v

DB_HOST = config['DB_HOST']
DB_PORT = int(config['DB_PORT'])
DB_NAME = config['DB_NAME']
DB_USERNAME = config['DB_USERNAME']
DB_PASSWORD = config['DB_PASSWORD']


