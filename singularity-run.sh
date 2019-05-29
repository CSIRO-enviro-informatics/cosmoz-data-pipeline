#!/bin/bash
#singularity-run.sh
#CONFIGURABLE VARS
COSMOZ_REST_API_HOST_PORT=8083
TIMESERIESDB_DIR="./tsdb"
DOCUMENTDB_DIR="./mgdb"
#END CONFIGURABLE VARS
if [ ! "$EUID" -eq "0" ]; then
    echo "This singularity orchestration script MUST be run as superuser."
    echo "Elevating previleges."
    echo "If requested, please input your sudo password, or press ctrl+c to cancel."
    sudo "$0" "$@"
    exit $?
fi
REBUILD=false
if [ "$1" = "stop" ]; then
    sudo singularity instance stop cosmoz-influx-inst
    sudo singularity instance stop cosmoz-mongo-inst
    sudo singularity instance stop cosmoz-rest-inst
    exit 0
fi
OLD_PWD="$PWD"
cd ./singularity
if [ "$1" = "rebuild" ]; then
    sudo singularity build mongodb.sif mongodb.Singularity
    sudo singularity build influxdb.sif influxdb.Singularity
    sudo singularity build cosmoz-app.sif application.Singularity
fi


if [ ! -f mongodb.sif ]; then
    echo "Building mongodb container image."
    sudo singularity build mongodb.sif mongodb.Singularity
fi
if [ ! -f influxdb.sif ]; then
    echo "Building influxdb container image."
    sudo singularity build influxdb.sif influxdb.Singularity
fi
if [ ! -f cosmoz-app.sif ]; then
    echo "Building cosmoz-rest-wrapper container image."
    sudo singularity build cosmoz-app.sif application.Singularity
fi
cd "$OLD_PWD"
echo "First trying to stop existing cosmoz mongodb container"
sudo singularity instance stop cosmoz-mongo-inst
SINGULARITYENV_MONGODB_RUN="/var/run/mongodb"
SINGULARITYENV_MONGODB_DATA="/var/lib/mongodb"
SINGULARITYENV_MONGODB_USER="mongodb"
SINGULARITYENV_MONGODB_IP="0.0.0.0"
SINGULARITYENV_MONGODB_OPTIONS="--journal"
sudo singularity instance start --cleanenv --writable-tmpfs --bind "${DOCUMENTDB_DIR}:/data/db" --net --network bridge --hostname "cosmoz.mongodb" ./singularity/mongodb.sif cosmoz-mongo-inst
T1_IP_ADDR="$(sudo singularity exec instance://cosmoz-mongo-inst ip addr|awk -F'[ \n\t/]+' '/global/ { print $3 }')"
echo "Cosmoz mongodb IP is: $T1_IP_ADDR"

echo "First trying to stop existing cosmoz influxdb container"
sudo singularity instance stop cosmoz-influx-inst
SINGULARITYENV_INFLUX_DB="cosmoz"
sudo singularity instance start --cleanenv --writable-tmpfs --bind "${TIMESERIESDB_DIR}:/var/lib/influxdb" --bind "influxdb.conf:/etc/influxdb/influxdb.conf" --net --network bridge --hostname "cosmoz.influxdb" ./singularity/influxdb.sif cosmoz-influx-inst
T2_IP_ADDR="$(sudo singularity exec instance://cosmoz-influx-inst ip addr|awk -F'[ \n\t/]+' '/global/ { print $3 }')"
echo "Cosmoz influxdb IP is: $T2_IP_ADDR"

echo "First trying to stop existing cosmoz rest wrapper container"
sudo singularity instance stop cosmoz-rest-inst
sudo rm -f ./singularity/dummy_hosts.txt
echo "127.0.0.1       localhost" > ./singularity/dummy_hosts.txt
echo "127.0.1.1       local.cosmoz.csiro.au      local" >> ./singularity/dummy_hosts.txt
echo "${T2_IP_ADDR}   influx.cosmoz.csiro.au     cosmoz.influxdb" >> ./singularity/dummy_hosts.txt
echo "${T1_IP_ADDR}   mongo.cosmoz.csiro.au      cosmoz.mongodb" >> ./singularity/dummy_hosts.txt
sudo singularity instance start --cleanenv --writable-tmpfs --net --network bridge --hostname "cosmoz.rest" --network-args "portmap=${COSMOZ_REST_API_HOST_PORT}:8080/tcp" --bind ./singularity/dummy_hosts.txt:/etc/hosts ./singularity/cosmoz-app.sif cosmoz-rest-inst




