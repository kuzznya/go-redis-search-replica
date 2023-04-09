#!/bin/bash

id=$1
[[ -z $id ]] && id=1
port=$((6379+id-1))
redis-cli -p $port "${@:2}"
