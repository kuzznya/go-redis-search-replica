#!/bin/zsh

set -eu

len=$(jq 'length' data.json)
for ((i=0;i<len;i++)) ; do
  id=$(jq -r ".[$i].id" data.json)
  hash=$(jq -r ".[$i] | to_entries | map(.key + \" \" + \"\\\"\" + (.value | tostring) + \"\\\"\") | join(\" \") " data.json)
  bash -cx "redis-cli -p 6379 -e HSET $id $hash"
done
