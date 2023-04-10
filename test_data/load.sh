#!/bin/zsh

set -eu

[[ -n $1 ]] && file=$1 || file=data.json

len=$(jq 'length' $file)
for ((i=0;i<len;i++)) ; do
  id=$(jq -r ".[$i].id" $file)
  hash=$(jq -r ".[$i] | to_entries | map(.key + \" \" + \"\\\"\" + (.value | tostring) + \"\\\"\") | join(\" \") " $file)
  bash -cx "redis-cli -p 6379 -e HSET $id $hash"
done
