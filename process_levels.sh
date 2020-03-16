#!/bin/bash
# Processes raw to level1, level1 to level2, level2 to level3, and level3 to level4
source ./.venv/bin/activate
PYTHONUNBUFFERED=1
export PYTHONUNBUFFERED

#Uncomment these to process to soils-discovery rather than in the container.
#export INFLUX_DB_HOST=soils-discovery.it.csiro.au
#export INFLUX_DB_PORT=8186
#export MONGO_DB_HOST=soils-discovery.it.csiro.au
#export MONGO_DB_PORT=27018

#This directory and this log file need to be present in order to run
mkdir -p "./res"
mkdir -p "./res/archive"
mkdir -p "./res/sbd-files"
touch "./res/debug.log"

# How many days to backprocess
PROCESS_DAYS=31
exec python3 - -d $PROCESS_DAYS <<'____HERE'
import sys
from datetime import datetime
from pipeline import cosmoz_process_levels
print("started process_levels.sh at {} local time".format(str(datetime.now())))
sys.exit(cosmoz_process_levels.main())
____HERE
