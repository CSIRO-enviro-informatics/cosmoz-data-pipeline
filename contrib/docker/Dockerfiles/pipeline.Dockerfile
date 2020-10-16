FROM alpine:3.9
MAINTAINER Ashley Sommer <Ashley.Sommer@csiro.au>
RUN echo "https://dl-3.alpinelinux.org/alpine/v3.9/main" >> /etc/apk/repositories
RUN echo "https://dl-3.alpinelinux.org/alpine/v3.9/community" >> /etc/apk/repositories
RUN apk add --no-cache --update bash tini-static python3 py3-virtualenv libuv libstdc++ gcompat freetds openssl curl
RUN apk add --no-cache --update --virtual buildenv git libuv-dev libffi-dev freetds-dev python3-dev openssl-dev py3-cffi build-base patchelf
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN patchelf --add-needed libgcompat.so.0 /usr/bin/python3.6
RUN pip3 install --upgrade "pip>=19.0.2"
RUN pip3 install --upgrade cython "setuptools>=40.8" "poetry>=1.1.0"
RUN echo 'manylinux1_compatible = True' > /usr/lib/python3.6/_manylinux.py &&\
    pip3 install "orjson==2.5.2" &&\
    rm /usr/lib/python3.6/_manylinux.py
WORKDIR /usr/local/lib
ARG COSMOZ_REST_WRAPPER_VER
RUN git clone https://github.com/CSIRO-enviro-informatics/cosmoz-rest-wrapper.git
ARG COSMOZ_DATA_PIPELINE_VER
RUN git clone https://github.com/CSIRO-enviro-informatics/cosmoz-data-pipeline.git
WORKDIR /usr/local/lib/cosmoz-data-pipeline
RUN python3 -m virtualenv -p /usr/bin/python3 --system-site-packages .venv
RUN source ./.venv/bin/activate &&\
    poetry run pip3 install --upgrade cython &&\
    poetry install -v --no-root &&\
    deactivate
WORKDIR /usr/local/lib/cosmoz-rest-wrapper
RUN python3 -m virtualenv -p /usr/bin/python3 --system-site-packages .venv
RUN source ./.venv/bin/activate &&\
    poetry install -v --no-root &&\
    poetry run pip3 install --upgrade git+git://github.com/esnme/ultrajson.git#egg=ujson &&\
    poetry run pip3 install "uvicorn>=0.12.0,<0.13.0" &&\
    deactivate
RUN apk del buildenv
ENV REST_API_INTERNAL_PORT=8080
ENV REST_API_LISTEN_HOST=127.0.0.1
ENV MONGODB_HOST=localhost
ENV MONGODB_PORT=27017
ENV INFLUXDB_HOST=localhost
ENV INFLUXDB_PORT=8086
ENTRYPOINT ["/sbin/tini-static", "--"]
CMD source ./.venv/bin/activate &&\
    cd src &&\
    poetry run uvicorn --host "$REST_API_LISTEN_HOST" --port "$REST_API_INTERNAL_PORT" --forwarded-allow-ips="*" app:app
