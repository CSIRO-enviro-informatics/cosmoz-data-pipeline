FROM alpine:3.9
ENV REST_API_INTERNAL_PORT=8080
ENV MONGODB_HOST=localhost
ENV MONGODB_PORT=27017
ENV INFLUXDB_HOST=localhost
ENV INFLUXDB_PORT=8086
RUN echo "https://dl-3.alpinelinux.org/alpine/v3.9/main" >> /etc/apk/repositories
RUN echo "https://dl-3.alpinelinux.org/alpine/v3.9/community" >> /etc/apk/repositories
RUN apk add --no-cache bash tini-static python3 py3-virtualenv libuv libstdc++ freetds curl
RUN apk add --no-cache --virtual buildenv git libuv-dev freetds-dev python3-dev build-base
RUN ln -s /usr/bin/python3 /usr/bin/python
WORKDIR /usr/local/lib
RUN git clone https://github.com/CSIRO-enviro-informatics/cosmoz-rest-wrapper.git
RUN git clone https://github.com/CSIRO-enviro-informatics/cosmoz-data-pipeline.git
WORKDIR /usr/local/lib/cosmoz-data-pipeline
RUN curl -L -o ./get-poetry.py https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py
RUN chmod +x ./get-poetry.py
RUN python3 ./get-poetry.py
ENV PATH="/root/.poetry/bin:${PATH}"
RUN poetry self update
RUN python3 -m virtualenv -p /usr/bin/python3 --system-site-packages .venv
RUN source ./.venv/bin/activate &&\
    poetry run pip3 install --upgrade cython &&\
    poetry install --no-root &&\
    deactivate
WORKDIR /usr/local/lib/cosmoz-rest-wrapper
RUN python3 -m virtualenv -p /usr/bin/python3 --system-site-packages .venv
RUN source ./.venv/bin/activate &&\
    poetry install --no-root &&\
    poetry run pip3 install --upgrade git+git://github.com/esnme/ultrajson.git#egg=ujson &&\
    poetry run pip3 install "gunicorn<20.0" &&\
    deactivate
RUN apk del buildenv
ENTRYPOINT ["/sbin/tini-static", "--"]
CMD source ./.venv/bin/activate &&\
    cd src && MY_CURRENT_IP="$(ip addr|awk -F'[ \n\t/]+' '/global/ { print $3 }')" &&\
    poetry run gunicorn app:app --bind "$MY_CURRENT_IP":"$REST_API_INTERNAL_PORT" --reuse-port --worker-class sanic.worker.GunicornWorker
