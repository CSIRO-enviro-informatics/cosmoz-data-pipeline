FROM alpine:3.9
ENV REST_API_INTERNAL_PORT=8080
ENV MONGODB_HOST=localhost
ENV MONGODB_PORT=27017
ENV INFLUXDB_HOST=localhost
ENV INFLUXDB_PORT=8086
RUN echo 'https://dl-3.alpinelinux.org/alpine/v3.9/main' >> /etc/apk/repositories
RUN echo 'https://dl-3.alpinelinux.org/alpine/v3.9/community' >> /etc/apk/repositories
RUN apk add --no-cache bash tini-static python3 py3-virtualenv libuv libstdc++ freetds
RUN apk add --no-cache --virtual buildenv git libuv-dev freetds-dev python3-dev build-base
WORKDIR /usr/local/lib
RUN git clone https://github.com/CSIRO-enviro-informatics/cosmoz-rest-wrapper.git
RUN git clone https://github.com/CSIRO-enviro-informatics/cosmoz-data-pipeline.git
WORKDIR /usr/local/lib/cosmoz-data-pipeline
RUN virtualenv -p python3 venv
RUN source ./venv/bin/activate &&\
    pip3 install --upgrade cython &&\
    cd ./scripts &&\
    pip3 install -r requirements.txt &&\
    deactivate
RUN touch ./_influx_db_config.py && touch ./_mongo_db_config.py && touch ./_sql_db_config.py
WORKDIR /usr/local/lib/cosmoz-rest-wrapper
RUN virtualenv -p python3 venv
RUN source ./venv/bin/activate &&\
    pip3 install -r requirements.txt &&\
    pip3 install --upgrade git+git://github.com/esnme/ultrajson.git#egg=ujson &&\
    pip3 install gunicorn &&\
    deactivate
RUN apk del buildenv
ENTRYPOINT ["/sbin/tini-static", "--"]
CMD source ./venv/bin/activate &&\
    cd src && MY_CURRENT_IP="$(ip addr|awk -F'[ \n\t/]+' '/global/ { print $3 }')" &&\
    gunicorn app:app --bind "$MY_CURRENT_IP":"$REST_API_INTERNAL_PORT" --reuse-port --worker-class sanic.worker.GunicornWorker
