// client.cpp
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>
#include <future>
#include <nlohmann/json.hpp>
#include <fstream>
#include "include/raft.grpc.pb.h"
#include "include/raft.h"
#include "include/kvserver.grpc.pb.h"
using json = nlohmann::json;
using namespace KV_Server;
using namespace raft;
using namespace grpc;

// 执行结果
struct ExecutedRequest {
   bool success;
   std::string value;
    
    ExecutedRequest() : success(true), value("") {}
    ExecutedRequest(bool s, const std::string& v) : success(s), value(v) {}
};

struct RequestKey {
   int64_t client_id;
   int64_t request_id;
    
    // 需要实现比较运算符（用于 map）
    bool operator<(const RequestKey& other) const {
        if (client_id != other.client_id) {
            return client_id < other.client_id;
        }
        return request_id < other.request_id;
    }
    
    // JSON 序列化
    json toJson() const {
        json j;
        j["client_id"] = client_id;
        j["request_id"] = request_id;
        return j;
    }
    
    // JSON 反序列化
   static RequestKey fromJson(const json& j) {
        RequestKey key;
        key.client_id = j.value("client_id", 0);
        key.request_id = j.value("request_id", 0);
        return key;
    }
};

struct RequestResult{
   bool success;          //
   std::string value;        // Get 操作的返回值
    
    // 默认构造函数
    RequestResult() : success(true), value("") {}
    
    // 带参构造函数
    RequestResult(const bool e, const std::string& v) : success(e), value(v) {}
    
    // JSON 序列化
    json toJson() const {
        json j;
        j["success"] = success;
        j["value"] = value;
        return j;
    }
    
    // JSON 反序列化
   static RequestResult fromJson(const json& j) {
        RequestResult result;
        result.success = j.value("success", true);
        result.value = j.value("value", "");
        return result;
    }
};

class KVServer : public KV_Server::KVService::Service {
public:
    KVServer(int me, const std::string& configFile) {
        raft_ = std::make_unique<Raft>(me, configFile);
        server_addresses = readKVConfig(configFile);
        me_ = me;
        // 获取 apply 通道并启动监听线程
        apply_ch = raft_->getApplyChannel();
        applier_thread_ = std::thread(&KVServer::applierLoop, this);

        // 线程启动Raft节点
        std::thread([this]() {
            raft_->Run();
        }).detach();
    }

    void Run() {
        if(me_< 0 || me_ >= server_addresses.size()) {
            throw std::runtime_error("Invalid node ID: " + std::to_string(me_));
        }
        std::string address = server_addresses[me_];
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        builder.RegisterService(this);
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        server->Wait();
    }
    grpc::Status Get(grpc::ServerContext* context,
                    const KV_Server::GetRequest* request,
                    KV_Server::GetResponse* response) override {
        RequestKey req_key{request->client_id(), request->request_id()};
        auto it = executed_requests_.find(req_key);
        
        if (it != executed_requests_.end()) {
            // 已执行过，直接返回缓存的结果
           std::cout << "Node " << me_ << ": Get request is duplicate, client_id=" 
                      << request->client_id() << ", request_id=" << request->request_id() << std::endl;
            response->set_success(it->second.success);
            response->set_value(it->second.value);
            return Status::OK;
        }
        int log_index = raft_->Start("get", request->key(), "");
        if ( log_index < 0 ) {
            response->set_success(false);
            response->set_err("ErrWrongLeader");
            return Status::OK;
        }

        // 等待命令被提交
        ExecutedRequest result = waitForApply(log_index);

        // 缓存结果
        executed_requests_[req_key] = result;

        response->set_success(result.success);
        response->set_value(result.value);
        return Status::OK;
    }
    grpc::Status PutAppend(grpc::ServerContext* context,
                    const KV_Server::PutAppendRequest* request,
                    KV_Server::PutAppendResponse* response) override {
        RequestKey req_key{request->client_id(), request->request_id()};
        auto it = executed_requests_.find(req_key);
        if (it != executed_requests_.end()) {
            // 已执行过，直接返回缓存的结果
           std::cout << "Node " << me_ << ": PutAppend request is duplicate, client_id=" 
                      << request->client_id() << ", request_id=" << request->request_id() << std::endl;
            response->set_success(it->second.success);
            return Status::OK;
        }

        // 提交raft
        std::string command = request->op();
        int log_index = raft_->Start(command, request->key(), request->value());

        if ( log_index < 0 ) {
            response->set_success(false);
            response->set_err("ErrWrongLeader");
            return Status::OK;
        }

        ExecutedRequest result = waitForApply(log_index);
        executed_requests_[req_key] = result;
        response->set_success(result.success);
        return Status::OK;
    }

private:
    int me_; // 当前节点的 ID
    std::unique_ptr<Raft> raft_;
    ApplyChannel* apply_ch;
    int leader_id = 0; // 假设 leader_id 是 0，实际应该通过状态查询获取
    std::vector<std::string> server_addresses;
    std::map<std::string, std::string> data_; // 简单的键值存储
    std::map<RequestKey, ExecutedRequest> executed_requests_; // 已执行请求的结果缓存
    
    // 等待应用的通知通道
    std::map<int, std::shared_ptr<std::promise<ExecutedRequest>>> notify_ch_;
    std::mutex notify_mutex_;

    // Applier 线程
    std::thread applier_thread_;
    std::atomic<bool> dead_;

    void applierLoop() {
        std::cout << "Node " << me_ << ": Applier thread started" << std::endl;
        while (!dead_) {
            // 从通道接收消息
            ApplyMsg msg = apply_ch->pop();
            std::cout << "Node " << me_ << ": Received apply message, index=" << msg.command_index << std::endl;
            if (!msg.command_valid) { 
                continue;
            }

            // 解析命令
            std::istringstream iss(msg.command);
            std::string op, key, value;
            iss >> op >> key >> value;
            if (op == "PUT" || op == "APPEND") {
                iss >> value;
            }
            // 执行命令
            ExecutedRequest result;
            if (op == "GET") {
                auto it = data_.find(key);
                if (it != data_.end()) {
                    result.value = it->second;
                    result.success = true;
                    std::cout << "Node " << me_ << ": GET command executed, key=" << key << ", value=" << value << std::endl;
                } else {
                    result.success = false;
                    result.value = "";
                    std::cout << "Node " << me_ << ": GET command executed, key=" << key << " not found" << std::endl;    
                }
            } else if (op == "PUT") {
                data_[key] = value;
                result.success = true;
                std::cout << "Node " << me_ << ": PUT command executed, key=" << key << ", value=" << value << std::endl;
            } else if (op == "APPEND") {
                data_[key] += value;
                result.success = true;
                std::cout << "Node " << me_ << ": APPEND command executed, key=" << key << ", value=" << data_[key] << std::endl;
            } else {
                result.success = false;
                std::cout << "Node " << me_ << ": Invalid command: " << op << std::endl;
            }

            // 通知等待的RPC handler
            notify(msg.command_index, result);
        }
    }

    ExecutedRequest waitForApply(int log_index) {
        auto promise = std::make_shared<std::promise<ExecutedRequest>>();
        {
            std::lock_guard<std::mutex> lock(notify_mutex_);
            notify_ch_[log_index] = promise;
        }

        auto future = promise->get_future();

        auto status = future.wait_for(std::chrono::seconds(5));
        if (status == std::future_status::ready) {
            return future.get();
        } else {
            std::cout << "Node " << me_ << ": Timeout waiting for apply message, log_index=" << log_index << std::endl;
            return ExecutedRequest{false, ""};
        }
    }

    // 通知等待者
    void notify(int log_index, const ExecutedRequest& result) {
        std::lock_guard<std::mutex> lock(notify_mutex_);
        auto it = notify_ch_.find(log_index);
        if (it != notify_ch_.end()) {
            it->second->set_value(result);
            notify_ch_.erase(it);
        }
    }
    std::vector<std::string> readKVConfig(const std::string& configFile) {
        std::ifstream file(configFile);
        if (!file.is_open()) {
            throw std::runtime_error("Cannot open config file: " + configFile);
        }
        
        json config;
        file >> config;
        
        std::vector<std::string> peerAddresses;
        for (const auto& node : config["nodes"]) {
            std::string address = node["ip"].get<std::string>() + ":" + std::to_string(node["port"].get<int>()+10);
            peerAddresses.push_back(address);
        }
        
        return peerAddresses;
    }
};

int main(int argc, char** argv) {
    if(argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <node_id> <node.json>" << std::endl;
        return 1;
    }
    int node_id = std::stoi(argv[1]);
    std::string configFile = argv[2];
    KVServer kvserver(node_id, configFile);
    kvserver.Run();

    return 0;
}