#!/bin/bash
# pre-populates the influxdb and mongodb from historical data

source ./scripts/venv/bin/activate
OLD_PWD="$PWD"
cd ./scripts
PYTHONUNBUFFERED=1
export PYTHONUNBUFFERED

# do all silo, intensities, and raw_vals (for 30 days)
python3 ./mssql-influx-converter.py -d 30

# do processing levels (for 30 days)
python3 ./cosmoz-process-levels.py -d 30

cd "$OLD_PWD"
