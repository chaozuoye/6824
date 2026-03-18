#!/bin/bash

# Raft集群启动脚本

# 检查是否提供了可执行文件路径
if [ $# -eq 0 ]; then
    echo "Usage: $0 <path_to_raft_executable> <path_to_config_file>"
    echo "Example: $0 ./build/raft node.json"
    exit 1
fi

RAFT_EXEC="$1"
CONFIG_FILE="$2"

# 检查可执行文件是否存在
if [ ! -f "$RAFT_EXEC" ]; then
    echo "Error: Executable '$RAFT_EXEC' not found!"
    exit 1
fi

# 检查配置文件是否存在
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file '$CONFIG_FILE' not found!"
    exit 1
fi

echo "Starting Raft cluster with executable: $RAFT_EXEC"
echo "Using config file: $CONFIG_FILE"
echo "====================================================="

# 启动5个节点 (根据你的node.json配置)
PIDS=()

# 启动节点0-4
for i in {0..4}; do
    echo "Starting node $i..."
    "$RAFT_EXEC" "$i" "$CONFIG_FILE" > "node_$i.log" 2>&1 &
    PIDS+=($!)
    echo "Node $i started with PID ${PIDS[-1]}"
done

echo "====================================================="
echo "All nodes started!"
echo "PIDs: ${PIDS[*]}"
echo ""
echo "To stop the cluster, run:"
echo "kill ${PIDS[*]}"
echo ""
echo "Log files are saved as node_*.log"

# 等待用户输入以保持脚本运行
echo ""
echo "Press Ctrl+C to stop all nodes"
trap "echo 'Stopping all nodes...'; kill ${PIDS[*]} 2>/dev/null; exit 0" INT TERM

# 等待所有进程结束
wait ${PIDS[*]}