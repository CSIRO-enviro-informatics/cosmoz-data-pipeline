Bootstrap: library
From: alpine:3.9
Stage: build

%files
    ./mongo-entrypoint.sh /entrypoint.sh

%environment
    export MONGO_INTERNAL_IP=27017

%post
    apk add --no-cache mongodb
    chmod +x /entrypoint.sh
    #VOLUME /data/db
    #EXPOSE 27017 28017

%runscript
    echo "Running application as executable."
    /entrypoint.sh "$@"

%startscript
    /entrypoint.sh mongod --bind_ip "0.0.0.0"
    
