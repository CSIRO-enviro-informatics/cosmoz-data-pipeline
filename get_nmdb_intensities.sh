#!/bin/bash
# Pulls down latest NMDB Intensities, puts them in the influxDB
source ./.venv/bin/activate
PYTHONUNBUFFERED=1
export PYTHONUNBUFFERED

#Uncomment these to upload to soils-discovery DB rather than in the container.
#export INFLUX_DB_HOST=soils-discovery.it.csiro.au
#export INFLUX_DB_PORT=8186
#export MONGO_DB_HOST=soils-discovery.it.csiro.au
#export MONGO_DB_PORT=27018

#This directory and this log file need to be present in order to run
mkdir -p "./res"
touch "./res/debug.log"

exec python3 - <<'____HERE'
import sys
from datetime import datetime
from nmdb import entrypoint
print("started get_nmdb_intensities.sh at {} local time".format(str(datetime.now())))
sys.exit(entrypoint.main())
____HERE
