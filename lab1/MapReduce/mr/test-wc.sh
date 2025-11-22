#!/bin/bash

# 清理之前的运行结果
rm -f mr-out-* mr-* diff.out master.log worker*.log

# 启动master
echo "启动master..."
./build/mr_master pg-*.txt > master.log 2>&1 &
MASTER_PID=$!
sleep 2

# 启动worker进程
echo "启动worker..."
./build/mr_worker > worker1.log 2>&1 &
WORKER1_PID=$!
./build/mr_worker > worker2.log 2>&1 &
WORKER2_PID=$!

# 等待处理完成，最多等待30秒
echo "等待处理完成..."
MAX_WAIT=30
WAIT_TIME=0
while [ $WAIT_TIME -lt $MAX_WAIT ]; do
    # 检查master是否仍在运行
    if ! kill -0 $MASTER_PID 2>/dev/null; then
        echo "Master进程已退出"
        break
    fi

    sleep 1
    WAIT_TIME=$((WAIT_TIME + 1))
done
# 检查是否生成了输出文件
if ls mr-out-* 1> /dev/null 2>&1; then
    echo "检测到输出文件"
fi
if [ $? -ne 0 ]; then
    echo "没有生成输出文件"
    kill $MASTER_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    wait $MASTER_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
    exit 1
fi

# 检查结果
if ls mr-out-* 1> /dev/null 2>&1; then
    echo "合并所有输出文件..."
    cat mr-out-* | sort > mrtmp.wcseq
    
    echo "比较结果..."
    sort -n -k2 mrtmp.wcseq | tail -10 | diff - mr_testout.txt > diff.out
    if [ -s diff.out ]
    then
        echo "Failed test. Output should be as in mr_testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
        cat diff.out
    else
        echo "Passed test" > /dev/stderr
    fi
else
    echo "测试失败：没有生成输出文件" > /dev/stderr
    echo "Master日志:" > /dev/stderr
    cat master.log > /dev/stderr
    echo "Worker日志:" > /dev/stderr
    cat worker1.log > /dev/stderr
fi

# 清理进程
kill $MASTER_PID $WORKER1_PID $WORKER2_PID 2>/dev/null
wait $MASTER_PID $WORKER1_PID $WORKER2_PID 2>/dev/null

# 清理临时文件
rm -f master.log worker*.log
rm -f mr-* mrtmp.wcseq