Bootstrap: library
From: alpine:3.9
Stage: build

%setup
    ORIG_PWD=${PWD}
    if cd ${ORIG_PWD}/cosmoz-data-pipeline; then git pull; else git clone https://github.com/CSIRO-enviro-informatics/cosmoz-data-pipeline.git ${ORIG_PWD}/cosmoz-data-pipeline; fi 
    if cd ${ORIG_PWD}/cosmoz-rest-wrapper; then git pull; else git clone https://github.com/CSIRO-enviro-informatics/cosmoz-rest-wrapper.git ${ORIG_PWD}/cosmoz-rest-wrapper; fi 

%files
    ./cosmoz-data-pipeline /usr/local/lib/cosmoz-data-pipeline
    ./cosmoz-rest-wrapper /usr/local/lib/cosmoz-rest-wrapper

%environment
    export REST_API_INTERNAL_PORT=8080
    export MONGODB_HOST=localhost
    export MONGODB_PORT=27017
    export INFLUXDB_HOST=localhost
    export INFLUXDB_PORT=8086

%post
    apk add --no-cache python3 py3-virtualenv libuv libstdc++
    apk add --no-cache --virtual buildenv git libuv-dev python3-dev build-base
    cd /usr/local/lib/cosmoz-rest-wrapper
    virtualenv -p python3 venv
    source ./venv/bin/activate
    pip3 install -r requirements.txt
    pip3 install --upgrade git+git://github.com/esnme/ultrajson.git#egg=ujson
    pip3 install gunicorn
    apk del buildenv

%runscript
    echo "Running application as executable."
    $0 "$@"

%startscript
    cd /usr/local/lib/cosmoz-rest-wrapper
    source ./venv/bin/activate
    cd src
    MY_CURRENT_IP="$(ip addr|awk -F'[ \n\t/]+' '/global/ { print $3 }')"
    gunicorn app:app --bind "$MY_CURRENT_IP":"$REST_API_INTERNAL_PORT" --reuse-port --worker-class sanic.worker.GunicornWorker
    

