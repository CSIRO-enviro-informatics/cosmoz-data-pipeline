#!/bin/bash

# start a development environment locally

docker-compose -f docker-compose.base.yml -f docker-compose.build.yml -f docker-compose.dev.yml up
