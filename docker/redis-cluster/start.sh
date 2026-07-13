#!/bin/sh
# Starts a 3-master Redis Cluster inside one container. Every node listens on and announces the same 127.0.0.1
# port, so the topology the clients discover is valid both between the nodes (same network namespace) and from the
# host (ports published 1:1).
set -e

PORTS="${REDIS_CLUSTER_PORTS:-7001 7002 7003}"

for port in $PORTS; do
  redis-server \
    --port "$port" \
    --cluster-enabled yes \
    --cluster-config-file "/tmp/nodes-$port.conf" \
    --cluster-node-timeout 5000 \
    --cluster-announce-ip 127.0.0.1 \
    --maxmemory 128mb \
    --maxmemory-policy allkeys-lru \
    --save '' \
    --appendonly no \
    --tcp-keepalive 60 \
    --logfile "/tmp/redis-$port.log" \
    --daemonize yes
done

for port in $PORTS; do
  until redis-cli -p "$port" ping > /dev/null 2>&1; do sleep 0.2; done
done

first="${PORTS%% *}"
if ! redis-cli -p "$first" cluster info | grep -q 'cluster_state:ok'; then
  nodes=""
  for port in $PORTS; do nodes="$nodes 127.0.0.1:$port"; done
  redis-cli --cluster create $nodes --cluster-yes --cluster-replicas 0
fi

# Keep the container in the foreground.
tail -f "/tmp/redis-$first.log"
