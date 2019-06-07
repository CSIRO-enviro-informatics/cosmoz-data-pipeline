#!/bin/bash
# pre-populates the influxdb and mongodb from historical data

source ./scripts/venv/bin/activate
OLD_PWD="$PWD"
cd ./scripts
PYTHONUNBUFFERED=1
export PYTHONUNBUFFERED
# do stations
python3 ./csv-mongodb-converter.py

# do all silo, intensities, and raw_vals (7300 is 20 years of data)
python3 ./mssql-influx-converter.py -d 7300

# do processing levels (7300 is backprocess 20 years)
python3 ./cosmoz-process-levels.py -d 7300


cd "$OLD_PWD"
