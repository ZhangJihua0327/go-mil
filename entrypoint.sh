#!/bin/sh

# entrypoint.sh - 根据 NODE_TYPE 环境变量启动相应的服务

set -e

if [ "$NODE_TYPE" = "replica" ]; then
    echo "Starting Replica node (ID: $REPLICA_ID)..."
    exec /root/replica
elif [ "$NODE_TYPE" = "tso" ]; then
    echo "Starting TSO node..."
    exec /root/tso
else
    echo "Error: NODE_TYPE must be set to 'tso' or 'replica'"
    exit 1
fi
