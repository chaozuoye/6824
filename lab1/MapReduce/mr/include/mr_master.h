#ifndef MR_MASTER_H
#define MR_MASTER_H

#include "mr_common.h"
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <grpcpp/grpcpp.h>

class Master {
public:
    // 构造函数：map任务数量、Reduce任务数量
    Master(int num_map_ = 10, int num_reduce = 10)num_map_(num_map_), num_reduce_(num_reduce);

    // 获取文件列表
    void GetAllFiles(char* file_list[], int argc);
    // 启动Master（循环等待Worker请求，调度任务）
    void Run();

    // 检查所有任务是否完成
    bool Done();

private:
    std::vector<std::string> files_;  // 输入文件列表
    int num_map_;                     // Map任务数量（等于输入文件数）
    int num_reduce_;                  // Reduce任务数量
    int file_num_;  // 文件数量

    // Map任务状态：task_id -> 状态 + 开始时间（用于超时检测）
    struct MapStatus {
        TaskState state;
        std::chrono::system_clock::time_point start_time;
    };
    std::map<int, MapStatus> map_tasks_;

    // Reduce任务状态：同上
    struct ReduceStatus {
        TaskState state;
        std::chrono::system_clock::time_point start_time;
    };
    std::map<int, ReduceStatus> reduce_tasks_;

    std::mutex mtx_;                  // 保护任务状态的锁
    bool all_done_;                   // 所有任务是否完成

    // 内部方法：为Worker分配一个任务（核心逻辑待实现）
    TaskResponse AssignTask();

    // 内部方法：标记某个Map任务完成（核心逻辑待实现）
    void MarkMapDone(int task_id);

    // 内部方法：标记某个Reduce任务完成（核心逻辑待实现）
    void MarkReduceDone(int task_id);

    // 内部方法：检查任务是否超时（超过10秒未完成则重新分配）
    void CheckTimeouts();
};

#endif // MR_MASTER_H