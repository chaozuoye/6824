#ifndef MR_COMMON_H
#define MR_COMMON_H

#include <vector>
#include <map>
#include <string>
#include <cstdint>
#include <cstring>
// // 任务类型：Map或Reduce
// enum TaskType {
//     MAP_TASK,
//     REDUCE_TASK,
//     NO_TASK  // 无可用任务（所有任务完成或等待中）
// };

// // 任务状态
// enum TaskState {
//     TASK_IDLE,    // 未分配
//     TASK_IN_PROGRESS,  // 执行中
//     TASK_DONE     // 已完成
// };

// // Map任务的输入：每个Map任务处理一个文件
// struct MapTask {
//     int task_id;          // 任务ID（唯一）
//     std::string filename; // 待处理的文件名
//     int num_reduce;       // Reduce任务总数（用于哈希分片）
// };

// // Reduce任务的输入：处理某个分片的中间结果
// struct ReduceTask {
//     int task_id;          // 任务ID（唯一）
//     int reduce_number;    // 分片编号（0~num_reduce-1）
//     int num_map;          // Map任务总数（用于读取所有Map的中间结果）
// };

// // Worker向Master请求任务的响应
// struct TaskResponse {
//     TaskType type;        // 任务类型（MAP/REDUCE/NO_TASK）
//     MapTask map_task;     // 若为MAP_TASK，填充此结构
//     ReduceTask reduce_task; // 若为REDUCE_TASK，填充此结构
// };

// // 中间结果的键值对（Map输出，Reduce输入）
// using IntermediatePair = std::pair<std::string, std::string>;

// // 用于生成中间结果文件名的工具函数（类似原实验的mr-X-Y）
// std::string intermediate_filename(int map_id, int reduce_id);

// // 用于生成最终结果文件名的工具函数（类似原实验的mr-out-Y）
// std::string output_filename(int reduce_id);

// 哈希函数：将key分配到某个Reduce分片（参考原实验的ihash）


// FNV-1a 32-bit hash implementation
int iHash(const std::string& key) {
    const uint32_t FNV_OFFSET_BASIS = 2166136261U;
    const uint32_t FNV_PRIME = 16777619U;
    
    uint32_t hash = FNV_OFFSET_BASIS;
    
    for (char c : key) {
        hash ^= static_cast<uint8_t>(c);
        hash *= FNV_PRIME;
    }
    
    return static_cast<int>(hash & 0x7fffffff);
}

#endif // MR_COMMON_H