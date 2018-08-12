#!/bin/bash

# Test against Synchrotron.
for pipeline in 1 4 8 16 32 64 128; do
  for clients in 1 2 4 8 16; do
    sleep 1
    request_count=$((pipeline*clients*5000))
    output=$(redis-benchmark -p 6380 -t get -n $request_count -c $clients -P $pipeline --csv)
    parsed=$(echo "$output" | awk -F'","' '{x=$2; gsub("\"","",x); print x}')
    echo "synchrotron $clients clients, $pipeline pipeline: $parsed"
  done
done

# Test against Twemproxy.
for pipeline in 1 4 8 16 32 64 128; do
  for clients in 1 2 4 8 16; do
    sleep 1
    request_count=$((pipeline*clients*5000))
    output=$(redis-benchmark -p 22121 -t get -n $request_count -c $clients -P $pipeline --csv)
    parsed=$(echo "$output" | awk -F'","' '{x=$2; gsub("\"","",x); print x}')
    echo "twemproxy $clients clients, $pipeline pipeline: $parsed"
  done
done
