#include "include/raft.h"
#include <vector>
#include <future>
#include <atomic>
#include <fstream>
#include <random>
#include <iostream>
#include <unistd.h>
#include <nlohmann/json.hpp>

using namespace raft;
using namespace grpc;
using json = nlohmann::json;

// 辅助函数：从JSON文件读取节点配置
std::vector<std::string> readNodeConfig(const std::string& configFile) {
    std::ifstream file(configFile);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open config file: " + configFile);
    }
    
    json config;
    file >> config;
    
    std::vector<std::string> peerAddresses;
    for (const auto& node : config["nodes"]) {
        std::string address = node["ip"].get<std::string>() + ":" + std::to_string(node["port"].get<int>());
        peerAddresses.push_back(address);
    }
    
    return peerAddresses;
}

// Raft类中的方法
Raft::Raft(int me, const std::string& configFile) 
    : me_(me),
      current_term_(0),
      voted_for_(-1),
      commit_index_(0),
      last_applied_(0),
      role_(Role::FOLLOWER),
      dead_(false) {
    
    // 从配置文件读取节点信息
    peerAddresses = readNodeConfig(configFile);
    
    // 创建到其他节点的gRPC通道
    for (int i = 0; i < peerAddresses.size(); i++) {
        if(i == me_) continue; // 跳过自己
        std::string addr = peerAddresses[i];
        auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        servers_.push_back(raft::RaftService::NewStub(channel));
    }
    
    // 初始化日志（索引0的占位符条目
    log_.reserve(1);
    
    // 设置随机选举超时（150-300ms）
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(150, 300);
    election_timeout_ = std::chrono::milliseconds(dis(gen));
    
    // 初始化心跳时间
    last_heartbeat_ = std::chrono::steady_clock::now();
    
    // 启动定时器线程
    ticker_thread_ = std::thread(&Raft::ticker, this);
}

Raft::~Raft() {
    dead_ = true;
    if (ticker_thread_.joinable()) {
        ticker_thread_.join();
    }
}

void Raft::ticker() {
    while (!dead_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto now = std::chrono::steady_clock::now();
        if (role_ != Role::LEADER && 
            now - last_heartbeat_ >= election_timeout_) {
            // 超过选举超时时间，发起选举
            std::cout <<"Node "<<me_ <<": Starting election..." << std::endl;
            startElection();
            std::cout <<"Node "<<me_ <<": Election completed" << std::endl;
        }
        if(role_ == Role::LEADER) {
            // 作为领导者，定期发送心跳
            for (int i = 0; i < servers_.size(); i++) {
                if (i != me_) {
                    AppendEntriesRequest request;
                    request.set_term(current_term_);
                    request.set_leader_id(me_);
                    request.set_prev_log_index(0);
                    request.set_prev_log_term(0);
                    request.set_leader_commit(commit_index_);
                    
                    AppendEntriesResponse response;
                    grpc::ClientContext context;
                    // 设置RPC超时时间，150毫秒
                    std::chrono::system_clock::time_point deadline = 
                        std::chrono::system_clock::now() + std::chrono::milliseconds(150);
                    context.set_deadline(deadline);
                    grpc::Status status = servers_[i]->AppendEntries(&context, request, &response);
                    // 处理响应...
                }
            }
        }
    }
}

void Raft::startElection() {
    // 增加任期并转为候选人
    current_term_++;
    voted_for_ = me_;
    role_ = Role::CANDIDATE;
    
    // 创建RequestVote请求
    RequestVoteRequest request;
    request.set_term(current_term_);
    request.set_candidate_id(me_);
    request.set_last_log_index(log_.size() - 1);
    request.set_last_log_term(0); // 简化处理
    
    // 并行向所有其他服务器发送请求
    std::vector<std::future<std::pair<raft::RequestVoteResponse, grpc::Status>>> futures;
    
    for (int i = 0; i < servers_.size(); i++) {
        if (i != me_ && servers_[i]) {
            // 为每个服务器创建异步调用
            auto stub = servers_[i].get(); // 获取或创建到服务器i的存根
            futures.push_back(
                std::async(std::launch::async, [stub, request]() {
                    raft::RequestVoteResponse response;
                    grpc::ClientContext context;
                    // 设置RPC超时时间，150毫秒
                    std::chrono::system_clock::time_point deadline = 
                        std::chrono::system_clock::now() + std::chrono::milliseconds(150);
                    context.set_deadline(deadline);
                    grpc::Status status = stub->RequestVote(&context, request, &response);
                    return std::make_pair(response, status);
                })
            );
        }
    }
    
    // 收集投票结果
    int votesReceived = 1; // 自己的一票
    std::cout << "Node "<<me_ <<": Received 1 vote (self)" << std::endl;
    
    for (auto& future : futures) {
        try {
            auto result = future.get();
            if (result.second.ok() && result.first.vote_granted()) {
                votesReceived++;
                std::cout <<"Node " <<me_ <<": Received 1 vote, total vote =  " << votesReceived << std::endl;
            }
            
            // 检查是否有服务器的任期更高
            if (result.first.term() > current_term_) {
                // 转为跟随者
                become_follower(result.first.term());
                return;
            }
        } catch (...) {
            // 处理超时或异常
            std::cerr << "RequestVote RPC failed" << std::endl;
        }
    }
    
    // 如果获得多数票，则成为领导者
    if (votesReceived > (servers_.size()+1) / 2) {
        become_leader();
    }
}

void Raft::become_follower(int term) {
    role_ = Role::FOLLOWER;
    std::cout << "Node " << me_ << ": Becoming follower for term " << term << std::endl;
    current_term_ = term;
    voted_for_ = -1;
}

void Raft::become_leader(){
    role_ = Role::LEADER;
    std::cout << "Node " << me_ << ": Becoming leader" << std::endl;
    // 初始化领导者特有状态
    next_index_.resize(servers_.size()+1, log_.size());
    match_index_.resize(servers_.size()+1, 0);
    
    // 发送初始的空日志条目作为心跳
    for (int i = 0; i < servers_.size(); i++) {
        if (i != me_ && servers_[i]) {
            AppendEntriesRequest request;
            request.set_term(current_term_);
            request.set_leader_id(me_);
            request.set_prev_log_index(0);
            request.set_prev_log_term(0);
            request.set_leader_commit(commit_index_);
            
            AppendEntriesResponse response;
            grpc::ClientContext context;
            // 设置RPC超时时间，150毫秒
            std::chrono::system_clock::time_point deadline = 
                std::chrono::system_clock::now() + std::chrono::milliseconds(150);
            context.set_deadline(deadline);
            grpc::Status status = servers_[i]->AppendEntries(&context, request, &response);
            // 处理响应...
        }
    }
}

std::tuple<int, int, bool> Raft::Start(const std::string& command) {
    // Implementation of Start method
    return {};
}

std::pair<int, bool> Raft::GetState() {
    // Implementation of GetState method
    return {current_term_, role_ == Role::LEADER};
}

void Raft::Kill() {
    dead_ = true;
}

grpc::Status Raft::RequestVote(grpc::ServerContext* context,
                            const raft::RequestVoteRequest* request,
                            raft::RequestVoteResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::cout << "Node " << me_ << ": Received vote request from " << request->candidate_id() 
              << " term " << request->term() << ", current term " << current_term_ << std::endl;
    
    if (request->term() < current_term_) {
        std::cout << "Node " << me_ << ": Reject vote - candidate term too old" << std::endl;
        response->set_term(current_term_);
        response->set_vote_granted(false);
        return Status::OK;
    }
    
    if (request->term() > current_term_) {
        std::cout << "Node " << me_ << ": Update term and become follower" << std::endl;
        become_follower(request->term());
    }
    
    // 简化投票条件检查，先测试基础功能
    bool canVote = (voted_for_ == -1 || voted_for_ == request->candidate_id());
    
    if (canVote) {
        std::cout << "Node " << me_ << ": Grant vote to " << request->candidate_id() << std::endl;
        voted_for_ = request->candidate_id();
        response->set_term(current_term_);
        response->set_vote_granted(true);
        last_heartbeat_ = std::chrono::steady_clock::now(); // 重置选举超时
    } else {
        std::cout << "Node " << me_ << ": Already voted for " << voted_for_ << std::endl;
        response->set_term(current_term_);
        response->set_vote_granted(false);
    }

    return Status::OK;
}

grpc::Status Raft::AppendEntries(grpc::ServerContext* context,
                              const raft::AppendEntriesRequest* request,
                              raft::AppendEntriesResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (request->term() < current_term_) {
        // 拒绝AppendEntries请求
        response->set_term(current_term_);
        response->set_success(false);
        return Status::OK;
    } else if (request->term() > current_term_) {
        become_follower(request->term());
    }
    
    last_heartbeat_ = std::chrono::steady_clock::now();
    // 处理日志条目追加逻辑
    response->set_term(current_term_);
    response->set_success(true);
    return Status::OK;
}

grpc::Status Raft::InstallSnapshot(grpc::ServerContext* context,
                                const raft::InstallSnapshotRequest* request,
                                raft::InstallSnapshotResponse* response) {
    return Status::OK;
}

void Raft::Run() {
    if(me_ < 0 || me_ >= peerAddresses.size()) {
        throw std::runtime_error("Invalid node ID: " + std::to_string(me_));
    }
    std::string server_address = peerAddresses[me_];
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Raft node " << me_ << " listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    if(argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <node_id> <node.json>" << std::endl;
        return 1;
    }
    std::unique_ptr<Raft> raftNode = std::make_unique<Raft>(atoi(argv[1]), argv[2]);
    raftNode->Run();
    return 0;
}