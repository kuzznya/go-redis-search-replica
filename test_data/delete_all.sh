#!/bin/bash

set -eu

[[ -n $1 ]] && file=$1 || file=data.json

for id in $(jq '.[].id' $file) ; do
  redis-cli -p 6379 DEL "$id"
done
