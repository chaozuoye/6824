#ifndef MR_WORKER_H
#define MR_WORKER_H

#include "mr_common.h"
#include <functional>

// Map函数类型：filename -> 中间键值对列表
using MapFunction = std::function<std::vector<IntermediatePair>(const std::string&)>;

// Reduce函数类型：key + 所有value -> 输出值（如"count"）
using ReduceFunction = std::function<std::string(const std::string&, const std::vector<std::string>&)>;

class Worker {
public:
    // 构造函数：传入用户定义的Map和Reduce函数
    Worker(MapFunction map_func, ReduceFunction reduce_func);

    // 启动Worker（循环向Master请求任务并执行）
    void Run(const std::string& master_addr);

private:
    MapFunction map_func_;    // 用户定义的Map函数
    ReduceFunction reduce_func_;  // 用户定义的Reduce函数

    // 执行Map任务（核心逻辑待实现）
    void DoMapTask(const MapTask& task);

    // 执行Reduce任务（核心逻辑待实现）
    void DoReduceTask(const ReduceTask& task);

    // 向Master请求任务（需要实现RPC通信）
    TaskResponse RequestTask(const std::string& master_addr);

    // 通知Master任务完成（需要实现RPC通信）
    void NotifyTaskDone(TaskType type, int task_id, const std::string& master_addr);
};

#endif // MR_WORKER_H