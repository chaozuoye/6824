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
#include <fstream>
#include "include/map_reduce.pb.h"
#include "include/map_reduce.grpc.pb.h"

using namespace mapreduce;
using namespace grpc;
class KeyValue
{
public:
    std::string key;
    std::string value;
};

typedef std::vector<KeyValue> (*MapFunc)(std::string filename);
typedef std::string (*ReduceFunc)(const std::string &key, const std::vector<std::string> &values);

int iHash(const std::string &key)
{
    const uint32_t FNV_OFFSET_BASIS = 2166136261U;
    const uint32_t FNV_PRIME = 16777619U;

    uint32_t hash = FNV_OFFSET_BASIS;

    for (char c : key)
    {
        hash ^= static_cast<uint8_t>(c);
        hash *= FNV_PRIME;
    }

    return static_cast<int>(hash & 0x7fffffff);
}

class Worker
{
public:
    Worker(std::shared_ptr<grpc::ChannelInterface> channel) : stub_(MasterService::NewStub(channel)) {}
    std::string generateMapResultFileName(int task_id, int reduce_id)
    {
        std::string filename = "mr-" + std::to_string(task_id) + "-" + std::to_string(reduce_id);
        return filename;
    }
    std::string generateReduceResultFileName(int reduce_id)
    {
        std::string filename = "mr-out-" + std::to_string(reduce_id);
        return filename;
    }
    void doMapTask(TaskResponse map_task)
    {
        TaskResponse request;
        Empty reply;
        ClientContext context;

        std::vector<KeyValue> map_output = mapF(map_task.filename());
        std::vector<std::vector<KeyValue>> grouped_data(map_task.num_reduce());
        for (const KeyValue &kv : map_output)
        {
            int reduce_id = iHash(kv.key) % map_task.num_reduce();
            grouped_data[reduce_id].push_back(kv);
            // writeIntermediate(kv, map_task.task_id(), reduce_id);
        }


        for (int reduce_id = 0; reduce_id < map_task.num_reduce(); reduce_id++)
        {
            if (!grouped_data[reduce_id].empty())
            {
                std::string filename = generateMapResultFileName(map_task.task_id(), reduce_id);
                writeIntermediate(filename, grouped_data[reduce_id]);
            }
        }

        report(map_task);
    }
    void doReduceTask(TaskResponse reduce_task)
    {
        std::map<std::string, std::vector<std::string>> reduce_output;
        for (int i = 0; i < reduce_task.num_map(); i++)
        {
            std::string filename = generateMapResultFileName(i, reduce_task.task_id());
            std::ifstream infile(filename, std::ios::in | std::ios::binary);
            if (!infile.is_open())
            {
                std::cerr << "Failed to open file: " << filename << std::endl;
                continue;
            }
            std::string line;
            while (std::getline(infile, line))
            {
                if (line.empty())
                    continue;
                
                if (!line.empty() && line.back() == '\r')
                {
                    line.pop_back();
                }
                std::string key, value;
                std::string::size_type pos = line.find(" ");
                key = line.substr(0, pos);
                value = line.substr(pos + 1);
                reduce_output[key].push_back(value);
            }
        }
        for (const auto &entry : reduce_output)
        {
            std::string key = entry.first;
            std::vector<std::string> values = entry.second;
            std::string value = reduceF(key, values);
            reduce_output[key] = {value};
        }
        writeReduceResult(generateReduceResultFileName(reduce_task.task_id()), reduce_output);
        report(reduce_task);
    }
    void report(TaskResponse task)
    {
        TaskResponse request;
        Empty reply;
        ClientContext context;
        request.set_task_type(task.task_type());
        request.set_task_id(task.task_id());
        Status status = stub_->report_task(&context, request, &reply);

        int count = 0;
        while (!status.ok() && count < 5)
        {
            status = stub_->report_task(&context, request, &reply);
            count++;
        }
        if (!status.ok())
        {
            std::cout << "report_task RPC failed." << std::endl;
            return;
        }
    }
    void writeReduceResult(std::string filename, std::map<std::string, std::vector<std::string>> &reduce_output)
    {
        std::string temp_filename = filename + ".tmp";
        std::ofstream outfile(temp_filename, std::ios::out | std::ios::binary);
        if (!outfile.is_open())
        {
            std::cerr << "Failed to create output file: " << filename << std::endl;
            return;
        }

        for (const auto &entry : reduce_output)
        {
            outfile << entry.first << " " << (entry.second)[0] << std::endl;
        }

        outfile.close();

        if (outfile.fail())
        {
            std::cerr << "Failed to write to temporary file: " << temp_filename << std::endl;
            std::remove(temp_filename.c_str());
            return;
        }
        
        if (std::rename(temp_filename.c_str(), filename.c_str()) != 0)
        {
            std::cerr << "Failed to rename temporary file to: " << filename << std::endl;
            std::remove(temp_filename.c_str());
        }
    }
    void writeIntermediate(std::string filename, std::vector<KeyValue> &kva)
    {
        std::string temp_filename = filename + ".tmp";

        std::ofstream outfile(temp_filename, std::ios::out | std::ios::binary);
        if (!outfile.is_open())
        {
            std::cerr << "Failed to create temporary file: " << temp_filename << std::endl;
            return;
        }

        for (const KeyValue &kv : kva)
        {
            outfile << kv.key << " " << kv.value << std::endl;
        }

        outfile.close();

        if (outfile.fail())
        {
            std::cerr << "Failed to write to temporary file: " << temp_filename << std::endl;
            std::remove(temp_filename.c_str());
            return;
        }

        if (std::rename(temp_filename.c_str(), filename.c_str()) != 0)
        {
            std::cerr << "Failed to rename temporary file to: " << filename << std::endl;
            std::remove(temp_filename.c_str());
        }
    }
    void Run()
    {
        while (true)
        {
            Empty request;
            TaskResponse reply;
            ClientContext context;
            Status status = stub_->getTask(&context, request, &reply);
            if (!status.ok()) {
                std::cout << "GetTask RPC failed." << std::endl;
                return;
            }
            if (reply.task_type() == TaskType::MAP_TASK) {
                doMapTask(reply);
            }
            else if (reply.task_type() == TaskType::REDUCE_TASK) {
                doReduceTask(reply);
            }
            else
            {
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

int main(int argc, char **argv)
{
    Worker worker(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
    HMODULE handle = LoadLibraryW(L"./tools.dll");
    if (!handle)
    {
        DWORD errorCode = GetLastError();
        std::cerr << "Cannot open library, error code: " << errorCode << std::endl;
        exit(-1);
    }
    // Windows下转换函数指针需要特殊处理 
    FARPROC mapProc = GetProcAddress(handle, "map_f");
    FARPROC reduceProc = GetProcAddress(handle, "reduce_f");

    if (!mapProc || !reduceProc)
    {
        std::cerr << "Cannot load symbols:" << std::endl;
        if (!mapProc) std::cerr << "  map_f not found" << std::endl;
        if (!reduceProc) std::cerr << "  reduce_f not found" << std::endl;
        FreeLibrary(handle);
        exit(-1);
    }

    worker.mapF = reinterpret_cast<MapFunc>(mapProc);
    worker.reduceF = reinterpret_cast<ReduceFunc>(reduceProc);
    worker.Run();
    return 0;
}