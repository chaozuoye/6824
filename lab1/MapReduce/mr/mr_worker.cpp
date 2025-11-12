#include "include/map_reduce.pb.h"
#include "include/map_reduce.grpc.pb.h"
#include "include/mr_common.h"
#include <iostream>
#include <memory>
#include <string>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <windows.h>
#include <vector>
#include <map>
using namespace mapreduce;
using namespace grpc;
//定义的两个函数指针用于动态加载动态库里的map和reduce函数
using IntermediatePair = std::pair<std::string, std::string>;
typedef std::vector<IntermediatePair> (*MapFunc)(std::string filename);
typedef std::string (*ReduceFunc)(const std::string& key, const std::vector<std::string>& values);
class Worker {
public:
    Worker(std::shared_ptr<grpc::ChannelInterface> channel) : stub_(MasterService::NewStub(channel)) {}
    std::string generateMapResultFileName(int task_id, int reduce_id){
        std::string filename = "mr-" + std::to_string(task_id)
            + "-" + std::to_string(reduce_id);
        return filename;
    }
    std::string generateReduceResultFileName(int reduce_id){ 
        std::string filename = "mr-out-" + std::to_string(reduce_id);
        return filename;
    }
    void doMapTask(TaskResponse map_task){
        TaskResponse request;
        Empty reply;
        ClientContext context;
        
        std::vector<IntermediatePair> map_output = mapF(map_task.filename());
        std::vector<std::vector<IntermediatePair>> grouped_data(map_task.num_reduce());
        for(const IntermediatePair& kv : map_output){
            int reduce_id = iHash(kv.first)%map_task.num_reduce();
            grouped_data[reduce_id].push_back(kv);
            // writeIntermediate(kv, map_task.task_id(), reduce_id);
        }

        // 原子写入每个分组的数据
        for (int reduce_id = 0; reduce_id < map_task.num_reduce(); reduce_id++) {
            if (!grouped_data[reduce_id].empty()) {
                std::string filename = generateMapResultFileName(map_task.task_id(), reduce_id);
                writeIntermediate(filename, grouped_data[reduce_id]);
            }
        }

        report(map_task);
    }
    void doReduceTask(TaskResponse reduce_task){
        std::map<std::string,std::vector<std::string>>reduce_output;
        for(int i = 0; i < reduce_task.num_map(); i++) {
            std::string filename = generateMapResultFileName(i, reduce_task.task_id());
            std::ifstream infile(filename, std::ios::in | std::ios::binary);
            if (!infile.is_open()) {
                std::cerr << "Failed to open file: " << filename << std::endl;
                continue;
            }
            std::string line;
            while (std::getline(infile, line)) {
                if (line.empty()) continue;
                // 处理可能的\r字符（Windows换行符）
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }
                std::string key, value;
                std::string::size_type pos = line.find(" ");
                key = line.substr(0, pos);
                value = line.substr(pos + 1);
                reduce_output[key].push_back(value);
            }

        }
        for (const auto& entry : reduce_output) {
            std::string key = entry.first;
            std::vector<std::string> values = entry.second;
            std::string value = reduceF(key, values);
            reduce_output[key] = {value};
        }
        writeReduceResult(generateReduceResultFileName(reduce_task.task_id()), reduce_output);
        report(reduce_task);
    }
    void report(TaskResponse task) {
        TaskResponse request;
        Empty reply;
        ClientContext context;
        request.set_task_type(task.type());
        request.set_task_id(task.task_id());
        request.set_task_state(TaskState::TASK_DONE);
        Status status = stub_->report_task(&context, request, &reply);
        /* 如果report_task失败，则重试5次*/
        int count = 0;
        while (!status.ok() && count < 5)
        {
            status = stub_->report_task(&context, request, &reply);
            count++;
        }
        if (!status.ok()) {
            std::cout << "report_task RPC failed." << std::endl;
            return;
        }
    }
    void writeReduceResult(std::string filename, std::map<std::string, std::vector<std::string>>&reduce_output) {
        std::string temp_filename = filename + ".tmp";
        std::ofstream outfile(temp_filename, std::ios::out | std::ios::binary);
        if (!outfile.is_open()) {
            std::cerr << "Failed to create output file: " << filename << std::endl;
            return;
        }
        
        for (const auto& entry : reduce_output) {
            outfile << entry.first << " " << (entry.second)[0] << std::endl;
        }

        outfile.close();
        // 检查写入是否成功
        if (outfile.fail()) {
            std::cerr << "Failed to write to temporary file: " << temp_filename << std::endl;
            std::remove(temp_filename.c_str());
            return;
        }
        
        // 原子地重命名为目标文件
        if (std::rename(temp_filename.c_str(), filename.c_str()) != 0) {
            std::cerr << "Failed to rename temporary file to: " << filename << std::endl;
            std::remove(temp_filename.c_str());
        }
    }
    void writeIntermediate(std::string filename, std::vector<IntermediatePair>&kva) {
        std::string temp_filename = filename + ".tmp";
        
        // 先写入临时文件
        std::ofstream outfile(temp_filename, std::ios::out | std::ios::binary);
        if (!outfile.is_open()) {
            std::cerr << "Failed to create temporary file: " << temp_filename << std::endl;
            return;
        }
        
        for (const IntermediatePair& kv : kva) {
            outfile << kv.first << " " << kv.second << std::endl;
        }
        
        outfile.close();
        
        // 检查写入是否成功
        if (outfile.fail()) {
            std::cerr << "Failed to write to temporary file: " << temp_filename << std::endl;
            std::remove(temp_filename.c_str());
            return;
        }
        
        // 原子地重命名为目标文件
        if (std::rename(temp_filename.c_str(), filename.c_str()) != 0) {
            std::cerr << "Failed to rename temporary file to: " << filename << std::endl;
            std::remove(temp_filename.c_str());
        }
    }
    void Run(){
        while(true){
            // 1. 获取任务
            Empty request;
            TaskResponse reply;
            ClientContext context;
            Status status = stub_->getTask(&context, request, &reply);
            if (!status.ok()) {
                std::cout << "GetTask RPC failed." << std::endl;
                return;
            }
            // 2. 执行任务
            if (reply.task_type() == TaskType::MAP_TASK) {
                doMapTask(reply);
            } else if (reply.task_type() == TaskType::REDUCE_TASK) {
                doReduceTask(reply);
            } else {
                std::cout << "No task." << std::endl;
            }
            Sleep(1000);
            
        }
    }
    MapFunc mapF;
    ReduceFunc reduceF;  
private:
    std::unique_ptr<MasterService::Stub> stub_;
};

int main(int argc, char** argv) {
    Worker worker(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
    //运行时从动态库中加载map及reduce函数(根据实际需要的功能加载对应的Func)
    HMODULE  handle = LoadLibrary(L"./tools.dll");
    if (!handle) {
        std::cerr << "Cannot open library: " << dlerror() << std::endl;
        exit(-1);
    }
    // Windows下转换函数指针需要特殊处理
    FARPROC mapProc = GetProcAddress(worker.handle, "map_f");
    FARPROC reduceProc = GetProcAddress(worker.handle, "reduce_f");
    
    if (!mapProc || !reduceProc) {
        std::cerr << "Cannot load symbols" << std::endl;
        FreeLibrary(worker.handle);
        exit(-1);
    }
    
    worker.mapF = reinterpret_cast<MapFunc>(mapProc);
    worker.reduceF = reinterpret_cast<ReduceFunc>(reduceProc);
    worker.Run();
    return 0;
}