FROM alpine:3.10
MAINTAINER Ashley Sommer <Ashley.Sommer@csiro.au>
LABEL maintainer="Ashley.Sommer@csiro.au"
RUN echo "https://dl-3.alpinelinux.org/alpine/v3.10/main" >> /etc/apk/repositories
RUN echo "https://dl-3.alpinelinux.org/alpine/v3.10/community" >> /etc/apk/repositories
RUN apk add --no-cache --update bash tini-static python3 py3-virtualenv libuv libstdc++ gcompat freetds openssl curl
RUN apk add --no-cache --update --virtual buildenv git libuv-dev libffi-dev freetds-dev python3-dev openssl-dev py3-cffi build-base patchelf
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN patchelf --add-needed libgcompat.so.0 /usr/bin/python3.7
RUN pip3 install --upgrade "pip>=19.0.2" "wheel"
RUN pip3 install --upgrade cython "setuptools>=40.8" "cryptography>3,<3.4" "poetry>=1.1.5"
RUN echo 'manylinux1_compatible = True' > /usr/lib/python3.7/_manylinux.py &&\
    pip3 install "orjson==2.5.2" &&\
    rm /usr/lib/python3.7/_manylinux.py
WORKDIR /usr/local/lib
ARG CLONE_BRANCH=master
ARG CLONE_ORIGIN="https://bitbucket.org/terndatateam/cosmoz-data-pipeline"
ARG CLONE_COMMIT=HEAD
RUN git clone --branch "${CLONE_BRANCH}" "${CLONE_ORIGIN}" src && mv ./src ./cosmoz-data-pipeline
WORKDIR /usr/local/lib/cosmoz-data-pipeline
RUN git checkout "${CLONE_COMMIT}"
RUN chmod -R 777 .
RUN addgroup -g 1000 -S cosmoz &&\
    adduser --disabled-password --gecos "" --home "$(pwd)" --ingroup "cosmoz" --no-create-home --uid 1000 cosmoz
USER cosmoz
RUN python3 -m virtualenv -p /usr/bin/python3 --system-site-packages .venv
RUN source ./.venv/bin/activate &&\
    poetry run pip3 install --upgrade cython &&\
    poetry install -v --no-root &&\
    deactivate
USER root
RUN apk del buildenv
USER cosmoz
ENV MONGODB_HOST=localhost
ENV MONGODB_PORT=27017
ENV INFLUXDB_HOST=localhost
ENV INFLUXDB_PORT=8086
ENTRYPOINT ["/sbin/tini-static", "--"]
# By default this container does nothing
CMD ["tail","-f","/dev/null"]
