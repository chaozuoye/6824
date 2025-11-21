#include "include/map_reduce.grpc.pb.h"
#include "include/map_reduce.pb.h"
#include "include/mr_common.h"
#include <iostream>
#include <memory>
#include <string>
#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>
#include <queue>
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
    bool selectTask(Task* task) {
        for (int i = 0; i < tasks.size(); i++) {
            if (tasks[i].task_state == TaskState::TASK_IDLE) {
                tasks[i].task_state = TaskState::TASK_IN_PROGRESS;
                tasks[i].start_time = std::time(nullptr);
                task = &tasks[i];
                return true;
            }
        }
        return false;
    }
    Status getTask(ServerContext* context, const Empty* request, TaskResponse* response) override{
        Task* task;
        if(selectTask(task)){
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
        if(request->task_type() == TaskType::MAP_TASK){
            if(request->task_type() == part_time){
                tasks[request->task_id()].task_state = TaskState::TASK_DONE;
            } else {
                return Status::CANCELLED;
            }
        }
        return Status::OK;
    }

    void GetAllFiles(char* file_list[], int argc)
    {
        tasks.clear();
        num_map_ = argc - 1;
        tasks.reserve(num_map_);
        for (int i = 1; i < argc; i++) {
            Task map_task;
            map_task.task_state = TaskState::TASK_IDLE;
            map_task.num_map = num_map_;
            map_task.num_reduce = num_reduce_;
            map_task.filename = file_list[i];
            map_task.task_id = i - 1;
            map_task.start_time = std::time(nullptr);
            tasks[i - 1] = map_task;
        }
    }

    void InitReduceTask()
    {
        tasks.clear();
        tasks.reserve(num_reduce_);
        for(int i = 0; i < num_reduce_; i++){
            Task reduce_tasks;
            reduce_tasks.task_state = TaskState::TASK_IDLE;
            reduce_tasks.num_map = num_map_;
            reduce_tasks.num_reduce = num_reduce_;
            reduce_tasks.task_id = i;
            reduce_tasks.start_time = std::time(nullptr);
            tasks[i] = reduce_tasks;
        }
    }

    void Run() {
        while(true) {
            all_done_ = true;
            for(auto& task: tasks) {
                if(task.task_state == TaskState::TASK_IN_PROGRESS && std::time(nullptr) - task.start_time > 10) {
                    task.task_state = TaskState::TASK_IDLE;
                    all_done_ = false;
                } else if(task.task_state == TaskState::TASK_IDLE) {
                    all_done_ = false;
                }
            }
            if(all_done_&& part_time == TaskType::REDUCE_TASK) {
                break;
            }
            if (all_done_)
            {
                part_time = TaskType::REDUCE_TASK;
                all_done_ = false;
                InitReduceTask();
            }
            Sleep(1000);
        }
    }
private:
    int num_reduce_;
    int num_map_;
    TaskType part_time = TaskType::MAP_TASK;
    bool all_done_ = false;
    std::vector<Task> tasks;
};


int main(int argc, char** argv) {
    if (argc != 2) {
        std::cout << "Usage: " << argv[0] << " filenames ..." << std::endl;
        return -1;
    }
    std::unique_ptr<Master> service = std::make_unique<Master>(10);
    service->GetAllFiles(argv, argc);
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on 0.0.0.0:50051" << std::endl;
    server->Wait();
    return 0;
}