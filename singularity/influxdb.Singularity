Bootstrap: library
From: alpine:3.9
Stage: build

%files
    ./default-influxdb.conf /etc/influxdb/influxdb.conf
    ./influx-entrypoint.sh /entrypoint.sh
    ./init-influxdb.sh /init-influxdb.sh

%environment
    export INFLUXDB_VERSION=1.7.6

%post
    echo 'hosts: files dns' >> /etc/nsswitch.conf
    apk add --no-cache tzdata bash ca-certificates && update-ca-certificates
    INFLUXDB_VERSION=1.7.6
    set -ex && \
    apk add --no-cache --virtual .build-deps wget gnupg tar && \
    for key in \
        05CE15085FC09D18E99EFB22684A14CF2582E0C5 ; \
    do \
        gpg --keyserver ha.pool.sks-keyservers.net --recv-keys "$key" || \
        gpg --keyserver pgp.mit.edu --recv-keys "$key" || \
        gpg --keyserver keyserver.pgp.com --recv-keys "$key" ; \
    done && \
    wget --no-verbose https://dl.influxdata.com/influxdb/releases/influxdb-${INFLUXDB_VERSION}-static_linux_amd64.tar.gz.asc && \
    wget --no-verbose https://dl.influxdata.com/influxdb/releases/influxdb-${INFLUXDB_VERSION}-static_linux_amd64.tar.gz && \
    gpg --batch --verify influxdb-${INFLUXDB_VERSION}-static_linux_amd64.tar.gz.asc influxdb-${INFLUXDB_VERSION}-static_linux_amd64.tar.gz && \
    mkdir -p /usr/src && \
    tar -C /usr/src -xzf influxdb-${INFLUXDB_VERSION}-static_linux_amd64.tar.gz && \
    rm -f /usr/src/influxdb-*/influxdb.conf && \
    chmod +x /usr/src/influxdb-*/* && \
    cp -a /usr/src/influxdb-*/* /usr/bin/ && \
    rm -rf *.tar.gz* /usr/src /root/.gnupg && \
    apk del .build-deps
    chmod +x /init-influxdb.sh
    chmod +x /entrypoint.sh

%runscript
    echo "Running application as executable."
    /entrypoint.sh "$@"

%startscript
    /entrypoint.sh influxd


