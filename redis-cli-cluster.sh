#!/bin/bash

set -e

id=$1
port=$((7000+id-1))

docker run --rm -it --network "${PWD##*/}_default" redis:7.0 redis-cli -h "redis-$id" -p "$port" "${@:2}"
