#!/bin/bash

# 启动单个Raft节点的脚本

if [ $# -eq 0]; then
    echo "Usage: $0 <node_id> <path_to_raft_executable> <path_to_config_file>"
    echo "Example: $0 0 ./build/raft node.json"
    exit 1
fi

NODE_ID=$1
RAFT_EXEC="$2"
CONFIG_FILE="$3"

if [ ! -f "$RAFT_EXEC" ]; then
    echo "Error: Executable '$RAFT_EXEC' not found!"
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file '$CONFIG_FILE' not found!"
    exit 1
fi

echo "Starting Raft node $NODE_ID"
"$RAFT_EXEC" "$NODE_ID" "$CONFIG_FILE" > "node_$NODE_ID.log" 2>&1 &