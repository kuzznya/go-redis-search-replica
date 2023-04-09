#!/bin/bash

set -eu

for id in $(jq '.[].id' data.json) ; do
  redis-cli -p 6379 DEL "$id"
done
