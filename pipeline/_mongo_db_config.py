# -*- coding: utf-8 -*-
#
import sys
from os import getenv
from .utils import do_load_dotenv

do_load_dotenv()

module = sys.modules[__name__]
defaults = module.defaults = {
    "DB_HOST": "documentdb",
    "DB_PORT": 27017,
    "DB_NAME": "cosmoz",
}
config = module.config = dict()

config['DB_HOST'] = getenv("MONGO_DB_HOST", None)
config['DB_PORT'] = getenv("MONGO_DB_PORT", None)
config['DB_NAME'] = getenv("MONGO_DB_NAME", None)

for k, v in defaults.items():
    if k not in config or config[k] is None:
        config[k] = v

DB_HOST = config['DB_HOST']
DB_PORT = int(config['DB_PORT'])
DB_NAME = config['DB_NAME']
