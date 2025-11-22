#include "include/map_reduce.grpc.pb.h"
#include "include/map_reduce.pb.h"
#include <iostream>
#include <memory>
#include <string>
#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>
#include <queue>
// 添加unistd.h头文件以支持sleep函数
#include <unistd.h>
// 添加glob.h以支持通配符匹配
#include <glob.h>
#include <mutex>
#include <thread>
using namespace mapreduce;
using namespace grpc;

struct Task{
    int task_id;
    int num_map;
    int num_reduce;
    TaskState task_state;
    std::string filename;
    std::time_t start_time;
};

class Master final : public mapreduce::MasterService::Service { 
public:
    Master(int num_reduce) : num_reduce_(num_reduce) {}
    
    bool selectTask(Task** task) {
        std::lock_guard<std::mutex> lock(task_mtx); // 自动管理锁
        for (int i = 0; i < tasks.size(); i++) {
            if (tasks[i].task_state == TaskState::TASK_IDLE) {
                tasks[i].task_state = TaskState::TASK_IN_PROGRESS;
                tasks[i].start_time = std::time(nullptr);
                *task = &tasks[i];
                return true;
            }
        }
        return false;
    }
    
    Status getTask(ServerContext* context, const Empty* request, TaskResponse* response) override{
        Task* task;
        if(selectTask(&task)){
            response->set_task_type(part_time);
            response->set_task_id(task->task_id);
            response->set_filename(task->filename);
            response->set_num_map(task->num_map);
            response->set_num_reduce(task->num_reduce);
        } else {
            response->set_task_type(TaskType::NO_TASK);
        }
        return Status::OK;
    }
    
    Status report_task(ServerContext* context, const TaskResponse* request, Empty* response) override{
        std::lock_guard<std::mutex> lock(task_mtx); // 自动管理锁
        if(request->task_type() == part_time && request->task_state() == TaskState::TASK_DONE){
            if (request->task_id() < 0 || request->task_id() >= tasks.size()) {
                std::cout << "Invalid task ID in report_task: " << request->task_id() << std::endl;
                return Status::CANCELLED;
            }
            tasks[request->task_id()].task_state = TaskState::TASK_DONE;
            std::cout << "Task type: " << request->task_type() << ", Task ID: "
                << request->task_id() << " reported done." << std::endl;
        } else {
            std::cout << "Task type mismatch in report_task or failed. dest type: " << part_time 
                << " current type: " << request->task_type() <<" Task State: "<< request->task_state()<<std::endl;
            return Status::CANCELLED;
        }
        return Status::OK;
    }

    void GetAllFiles(char* file_patterns[], int argc)
    {
        std::lock_guard<std::mutex> lock(task_mtx); // 自动管理锁
        tasks.clear();
        std::vector<std::string> matched_files;
        
        // 解析命令行参数，展开通配符
        for (int i = 1; i < argc; i++) {
            glob_t glob_result;
            int ret = glob(file_patterns[i], GLOB_TILDE, NULL, &glob_result);
            
            if (ret == 0) {
                // 成功匹配到文件
                for (size_t j = 0; j < glob_result.gl_pathc; ++j) {
                    matched_files.push_back(std::string(glob_result.gl_pathv[j]));
                    std::cout << "filename: " << glob_result.gl_pathv[j] << std::endl;
                }
            } else if (ret == GLOB_NOMATCH) {
                std::cout << "No match found for pattern: " << file_patterns[i] << std::endl;
            }
            // 其他情况（如GLOB_ABORTED等）忽略
            
            globfree(&glob_result);
        }
        
        num_map_ = matched_files.size();
        tasks.reserve(num_map_);
        
        for (size_t i = 0; i < matched_files.size(); i++) {
            Task map_task;
            map_task.task_state = TaskState::TASK_IDLE;
            map_task.num_map = num_map_;
            map_task.num_reduce = num_reduce_;
            map_task.filename = matched_files[i];
            map_task.task_id = i;
            map_task.start_time = std::time(nullptr);
            tasks.push_back(map_task);
        }
    }

    void InitReduceTask()
    {
        tasks.clear();
        tasks.reserve(num_reduce_);
        for(int i = 0; i < num_reduce_; i++){
            Task reduce_task;
            reduce_task.task_state = TaskState::TASK_IDLE;
            reduce_task.num_map = num_map_;
            reduce_task.num_reduce = num_reduce_;
            reduce_task.task_id = i;
            reduce_task.start_time = std::time(nullptr);
            tasks.push_back(reduce_task);
        }
    }
    
    void StartRunThread() {
        run_thread_ = std::thread([this]() {
            Run();
        });
    }
    
    // 新增一个等待Run线程结束的方法
    void JoinRunThread() {
        if (run_thread_.joinable()) {
            run_thread_.join();
        }
    }
    void ShutdownServer() {
        should_shutdown_ = true;
    }
    
    bool ShouldShutdown() {
        std::lock_guard<std::mutex> lock(shutdown_mtx_);
        return should_shutdown_;
    }
    void Run() {
        while(true) {
            {
                std::lock_guard<std::mutex> lock(task_mtx); // 使用作用域限制锁的范围
                all_done_ = true;
                for(auto& task: tasks) {
                    if(task.task_state == TaskState::TASK_IN_PROGRESS && std::time(nullptr) - task.start_time > 10) {
                        task.task_state = TaskState::TASK_IDLE;
                        all_done_ = false;
                    } else if(task.task_state == TaskState::TASK_IDLE) {
                        all_done_ = false;
                    }
                }
                if(all_done_ && part_time == TaskType::REDUCE_TASK) {
                    // 删除中间文件
                    for(int i = 0; i < num_map_; i++) {
                        for(int j = 0; j < num_reduce_; j++) {
                            std::string filename = "mr-" + std::to_string(i) + "-" + std::to_string(j);
                            if (std::remove(filename.c_str()) != 0) {
                                std::cerr << "Error deleting file: " << filename << std::endl;
                            }
                        }
                    }
                    std::cout << "All tasks completed. Master exiting." << std::endl;
                    std::lock_guard<std::mutex> shutdown_lock(shutdown_mtx_);
                    should_shutdown_ = true;
                    break;
                }
                if (all_done_) {
                    part_time = TaskType::REDUCE_TASK;
                    all_done_ = false;
                    InitReduceTask();
                }
            } // 锁在这里自动释放
            sleep(1);
        }
    }
    
private:
    int num_reduce_;
    int num_map_;
    TaskType part_time = TaskType::MAP_TASK;
    bool all_done_ = false;
    std::vector<Task> tasks;
    std::mutex task_mtx; // 用于保护共享数据的互斥量
    std::thread run_thread_;
    // 用于控制服务器关闭的变量
    bool should_shutdown_ = false;
    std::mutex shutdown_mtx_;
};

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <file1> [file2] ..." << std::endl;
        std::cerr << "       " << argv[0] << " pg-*.txt" << std::endl;
        return 1;
    }
    std::unique_ptr<Master> service = std::make_unique<Master>(10);
    service->GetAllFiles(argv, argc);
    service->StartRunThread();
    
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on 0.0.0.0:50051" << std::endl;
    
    // 定期检查是否应该关闭服务器
    while (!service->ShouldShutdown()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    std::cout << "Shutting down server..." << std::endl;
    server->Shutdown();
    
    // 等待Run线程结束
    service->JoinRunThread();
    return 0;
}