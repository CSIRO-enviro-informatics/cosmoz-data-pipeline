#!/bin/bash
# Processes raw to level1, level1 to level2, level2 to level3, and level3 to level4
source ./venv/bin/activate
PYTHONUNBUFFERED=1
export PYTHONUNBUFFERED
#export INFLUX_DB_HOST=soils-discovery.it.csiro.au
#export INFLUX_DB_PORT=8186
#export MONGO_DB_HOST=soils-discovery.it.csiro.au
#export MONGO_DB_PORT=27018

# How many days to backprocess
PROCESS_DAYS=31
exec python3 - -d $PROCESS_DAYS <<'____HERE'
import sys
from pipeline import cosmoz_process_levels
sys.exit(cosmoz_process_levels.main())
____HERE
