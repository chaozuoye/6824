#include "include/raft.h"
#include <vector>
#include <future>
#include <atomic>
#include <fstream>
#include <random>
#include <iostream>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include "raft.h"

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
        leader_id_(-1),
        current_term_(0),
        voted_for_(-1),
        commit_index_(-1),
        last_applied_(-1),
        role_(Role::FOLLOWER),
        dead_(false) {
    
    // 从配置文件读取节点信息
    peerAddresses = readNodeConfig(configFile);
    
    // 创建到其他节点的gRPC通道
    for (int i = 0; i < peerAddresses.size(); i++) {
        if(i == me_) {
            servers_.push_back(nullptr);    // 创建空指针
            continue;
        }
        std::string addr = peerAddresses[i];
        auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        servers_.push_back(raft::RaftService::NewStub(channel));
    }
    
    // 初始化日志
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
    int count = 0;
    while (!dead_) {
        count = (count + 1) % 60;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        {
            std::lock_guard<std::mutex> lock(mutex_);
            
            auto now = std::chrono::steady_clock::now();
            if (role_ != Role::LEADER && 
                now - last_heartbeat_ > election_timeout_) {
                // 超过选举超时时间，发起选举
                std::cout <<"Node "<<me_ <<": Starting election..." << std::endl;
                startElection();
                std::cout <<"Node "<<me_ <<": Election completed" << std::endl;
            }else if(role_ == Role::LEADER) {
                // 作为领导者，定期发送心跳
                last_heartbeat_ = now;
                sendHeartbeatsAsync();
                if(count == 0){
                    std::cout <<"Node "<<me_ <<": Sent heartbeat" << std::endl;
                }
            }
        }
    }
}

void Raft::sendHeartbeatsAsync() {
    std::vector<std::future<void>> futures;
    
    for (int i = 0; i < servers_.size(); i++) {
        if (i != me_ && servers_[i]) {
            futures.push_back(
                std::async(std::launch::async, [this, i]() {
                    // 构造心跳包（空的AppendEntries请求）
                    AppendEntriesRequest request;
                    {
                        request.set_term(current_term_);
                        request.set_leader_id(me_);
                        
                        // 心跳包的prev_log_index和prev_log_term使用当前日志状态
                        int last_log_index = log_.size() - 1;
                        request.set_prev_log_index(last_log_index);
                        request.set_prev_log_term(last_log_index >= 0 ? log_[last_log_index].term() : 0);
                        request.set_leader_commit(commit_index_);
                        // 不包含entries，这就是心跳包
                    }
                    
                    AppendEntriesResponse response;
                    grpc::ClientContext context;
                    
                    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(150);
                    context.set_deadline(deadline);
                    
                    grpc::Status status = servers_[i]->AppendEntries(&context, request, &response);
                    
                    if (!status.ok()) {
                        std::cout << "Heartbeat to server " << i << " failed: " << status.error_message() << std::endl;
                    }
                })
            );
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
    request.set_last_log_term(log_.empty() ? 0 : log_.back().term());
    
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
    last_heartbeat_ = std::chrono::steady_clock::now();
}

void Raft::become_leader(){
    role_ = Role::LEADER;
    std::cout << "Node " << me_ << ": Becoming leader" << std::endl;
    // 初始化领导者特有状态
    next_index_.resize(servers_.size(), log_.size());
    match_index_.resize(servers_.size(), -1);

    // 发送初始心跳以建立领导者地位
    sendHeartbeatsAsync();
}

std::tuple<int, int, bool> Raft::Start(const std::string& command) {
    // Implementation of Start method
    // if (role_ == Role::LEADER) {
    //     // 领导者处理请求
    //     int index = log_.size();
    //     log_.emplace_back(command, index, current_term_);
        
    //     // 创建AppendEntries请求
    //     AppendEntriesRequest request;
    //     request.set_term(current_term_);
    //     request.set_leader_id(me_);
    //     request.set_prev_log_index(index - 1);
    // } else {
    //     // 非领导者处理请求
    //     return {0, 0, false};
    // }
    return {};
}

std::pair<int, bool> Raft::GetState() {
    // Implementation of GetState method
    return {current_term_, role_ == Role::LEADER};
}

void Raft::Kill() {
    dead_ = true;
}

bool Raft::isCandidateLogUpToDate(const raft::RequestVoteRequest* request) {
    int lastLogIndex = log_.size() - 1;
    int lastLogTerm = log_.empty() ? 0 : log_.back().term(); // 最后一个日志条目的任期
    
    // Raft论文中的up-to-date判断规则：
    // 1. 如果任期不同，任期大的更新
    // 2. 如果任期相同，日志更长的更新
    if (request->last_log_term() > lastLogTerm) {
        return true;
    } else if (request->last_log_term() < lastLogTerm) {
        return false;
    } else {
        // 任期相同，比较索引
        return request->last_log_index() >= lastLogIndex;
    }
}

grpc::Status Raft::RequestVote(grpc::ServerContext* context,
                            const raft::RequestVoteRequest* request,
                            raft::RequestVoteResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::cout << "Node " << me_ << ": Vote request from " << request->candidate_id()
              << " (term=" << request->term() << ")" << std::endl;
    
    // 1. 任期检查
    if (request->term() < current_term_) {
        std::cout << "Node " << me_ << ": Reject - candidate term too old" << std::endl;
        response->set_term(current_term_);
        response->set_vote_granted(false);
        last_heartbeat_ = std::chrono::steady_clock::now();
        return Status::OK;
    }
    
    // 2. 如果候选人的任期更高，更新自己的任期并转为跟随者
    if (request->term() > current_term_) {
        std::cout << "Node " << me_ << ": Higher term detected, becoming follower" << std::endl;
        become_follower(request->term());
    }
    
    // 3. 检查是否已投票
    bool alreadyVoted = (voted_for_ != -1 && voted_for_ != request->candidate_id());
    
    // 4. 检查候选人日志是否足够新
    bool candidateLogUpToDate = isCandidateLogUpToDate(request);
    
    // 5. 决定是否投票
    if (!alreadyVoted && candidateLogUpToDate) {
        std::cout << "Node " << me_ << ": Grant vote to " << request->candidate_id() << std::endl;
        voted_for_ = request->candidate_id();
        response->set_term(current_term_);
        response->set_vote_granted(true);
    } else {
        std::cout << "Node " << me_ << ": Reject vote - ";
        if (alreadyVoted) {
            std::cout << "already voted for " << voted_for_ << std::endl;
        } else {
            std::cout << "candidate log not up-to-date" << std::endl;
        }
        response->set_term(current_term_);
        response->set_vote_granted(false);
    }
    last_heartbeat_ = std::chrono::steady_clock::now(); // 重置选举超时
    return Status::OK;
}

grpc::Status Raft::AppendEntries(grpc::ServerContext* context,
                              const raft::AppendEntriesRequest* request,
                              raft::AppendEntriesResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    static int heartbeat_count = 0;
    // 任期检查
    if (request->term() < current_term_) {
        response->set_success(false);
        response->set_term(current_term_);
        return Status::OK;
    }
    
    // 如果请求的任期更高，更新自己的任期并转为跟随者
    if (request->term() > current_term_) {
        become_follower(request->term());
    }
    last_heartbeat_ = std::chrono::steady_clock::now();
    leader_id_ = request->leader_id();
    
    // 判断是否是心跳
    if (request->entries_size() == 0) {
        response->set_success(true);
        response->set_term(current_term_);
        heartbeat_count = (heartbeat_count + 1) % 60;
        if(heartbeat_count == 0) {
            std::cout << "Node " << me_ << ": Received heartbeat from leader " << request->leader_id() << std::endl;
        }
        return Status::OK;
    } else {
        std::cout << "Node " << me_ << ": Received append entries from leader " << request->leader_id() << std::endl;
    }
    
    // 检查prevLogIndex是否有效
    if (request->prev_log_index() >= static_cast<int>(log_.size()) && log_.size()>0) {
        std::cout << "Node " << me_ << ": Invalid prevLogIndex " << request->prev_log_index() << ", log size " << log_.size() << std::endl;
        response->set_success(false);
        response->set_term(current_term_);
        response->set_conflict_index(log_.size() - 1);
        response->set_conflict_term(log_.empty() ? 0 : log_.back().term());
        return Status::OK;
    }
    
    // 检查prevLogTerm是否匹配或者当前日记数量大于prevLogIndex
    if ((request->prev_log_index() >= 0 && 
        log_[request->prev_log_index()].term() != request->prev_log_term())||
        static_cast<int>(log_.size()) > request->prev_log_index()+1) {
        std::cout<<"prev_log_index: "<<request->prev_log_index()<<", log size: "<<log_.size()
                <<" prev_log_term: "<<request->prev_log_term()<<std::endl;
        response->set_success(false);
        response->set_term(current_term_);
        
        // 提供冲突信息帮助领导者快速定位
        int conflict_index = request->prev_log_index() < 0 ? 0 : request->prev_log_index();
        int conflict_term = log_[conflict_index].term();
        response->set_conflict_index(conflict_index);
        response->set_conflict_term(conflict_term);
        
        // 删除冲突的日志条目
        log_.erase(log_.begin() + conflict_index, log_.end());
        std::cout << "Node " << me_ << ": Removed conflicting log entries" << std::endl;
        return Status::OK;
    }
    
    // 处理日志条目追加逻辑
    response->set_term(current_term_);
    response->set_success(true);
    for(int i = 0; i < request->entries_size(); i++) {
        const LogEntry& entry = request->entries(i);
        // 追加新条目
        log_.push_back(entry);
    }
    std::cout << "Node " << me_ << ": Appended log entry from leader " << request->leader_id() << std::endl;
    // 打印当前所有日志
    printLog();
    return Status::OK;
}
void Raft::printLog() {
    std::cout << "Log: ";
    for (const auto& entry : log_) {
        std::cout << "(" << entry.term() << ", " << entry.command() << ") ";
    }
    std::cout << std::endl;
}

grpc::Status Raft::InstallSnapshot(grpc::ServerContext* context,
                                const raft::InstallSnapshotRequest* request,
                                raft::InstallSnapshotResponse* response) {
    return Status::OK;
}

grpc::Status Raft::SendCommand(grpc::ServerContext* context,
                                const raft::CommandRequest* request,
                                raft::CommandResponse* response) {
    // 首先检查角色，但不阻塞太久
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (role_ == Role::LEADER) {
            std::cout << "Node " << me_ << ": Received command from client" << std::endl;
            return handleCommandAsLeader(request, response);
        }
        
        if (leader_id_ != -1) {
            // 已知领导者，保存领导者ID
            int known_leader = leader_id_;
            // 释放锁后转发
            std::cout << "Node " << me_ << ": Forwarding command to leader " << known_leader << std::endl;
            return forwardCommandToLeader(known_leader, request, response);
        }
    }
    
    // 不知道领导者，短暂等待后再次检查
    for (int i = 0; i < 5; ++i) {
        std::cout << "Node " << me_ << ": Waiting for leader to be known" << std::endl;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (leader_id_ != -1) {
                return forwardCommandToLeader(leader_id_, request, response);
            }
        }
    }
    

    response->set_success(false);
    response->set_leader_id(-1);
    return Status::CANCELLED;
}

grpc::Status Raft::handleCommandAsLeader(const raft::CommandRequest* request,
                                         raft::CommandResponse* response) {
    // 在领导者角色下处理命令
    LogEntry new_entry;
    new_entry.set_command(request->command());
    new_entry.set_index(log_.size());
    new_entry.set_term(current_term_);
    
    log_.push_back(new_entry);
    //persist();  // 持久化日志
    
     // 线程启动日志复制流程
    std::thread([this, new_entry]() {
        printLog();
        replicateLogEntry(new_entry.index());
    }).detach();
    
    response->set_success(true);
    response->set_leader_id(me_);
    
    return Status::OK;
}

bool Raft::replicateLogEntry(int log_index) {
    // 发送AppendEntries给所有跟随者
    std::vector<std::future<bool>> futures;
    
    for (int i = 0; i < servers_.size(); i++) {
        if (i != me_ && servers_[i]) {
            futures.push_back(
                std::async(std::launch::async, [this, i, log_index]() {
                    // 尝试发送三次，若三次失败则返回false
                    for (int j = 0; j < 3; j++) {
                        if (sendLogEntryToServer(i, log_index)) {
                            return true;
                        }
                    }
                    std::cout << "Node " << me_ << ": Failed to replicate log entry to server " << i << std::endl;
                    return false;
                })
            );
        }
    }
    
    // 等待响应，统计成功数量
    int success_count = 1; // 自己算一票
    
    for (auto& future : futures) {
        try {
            if (future.get()) {
                success_count++;
            }
        } catch (...) {
            // 处理异常情况
        }
    }
    
    // 如果多数节点确认，提交日志
    if (success_count > (servers_.size() + 1) / 2) {
        commitLogUpTo(log_index);
        return true;
    }
    
    return false;
}

bool Raft::sendLogEntryToServer(int server_id, int log_index) {
    // 构造AppendEntries请求，包含特定的日志条目
    AppendEntriesRequest request;
    
    request.set_term(current_term_);
    request.set_leader_id(me_);

    // 使用next_index_确定prevLogIndex
    int prev_index = next_index_[server_id] - 1;
    std::cout << "Node " << server_id << " next_index: " << next_index_[server_id] << std::endl;
    request.set_prev_log_index(prev_index);
    request.set_prev_log_term(prev_index < 0? 0 : log_[prev_index].term());
    for(int i = prev_index + 1; i < log_.size(); i++) {
        auto* entry = request.add_entries();
        entry->set_term(log_[i].term());
        entry->set_command(log_[i].command());
        entry->set_index(log_[i].index());
    }
    
    AppendEntriesResponse response;
    grpc::ClientContext context;
    
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    
    grpc::Status status = servers_[server_id]->AppendEntries(&context, request, &response);
    
    if (status.ok() && response.success()) {
        // 更新该服务器的匹配信息
        if (server_id < match_index_.size()) {
            match_index_[server_id] = log_index;
            if (server_id < next_index_.size()) {
                next_index_[server_id] = log_index + 1;
            }
        }
        return true;
    } else {
        // 失败时根据conflict_index进行退避然后重新发送
        int conflict_index = response.conflict_index();
        if (conflict_index > 0) {
            if (server_id < next_index_.size()) {
                next_index_[server_id] = std::max(0, conflict_index);
            }
        }
        return false;
    }
}

void Raft::commitLogUpTo(int index) {
    
    if (index > commit_index_) {
        commit_index_ = index;
        std::cout << "Node " << me_ << ": Committed log up to index " << index << std::endl;
        
        // 应用已提交的日志到状态机
        applyCommittedLogs();
    }
}

void Raft::applyCommittedLogs() {
    // 应用从last_applied_到commit_index_之间的日志
    for (int i = last_applied_ + 1; i <= commit_index_; i++) {
        if (i < log_.size()) {
            // 在这里应用日志到状态机
            std::cout << "Node " << me_ << ": Applying log entry " << i 
                      << " with command: " << log_[i].command() << std::endl;
            last_applied_ = i;
        }
    }
}
grpc::Status Raft::forwardCommandToLeader(int leader_node_id,
                                          const raft::CommandRequest* request,
                                          raft::CommandResponse* response) {
    if (leader_node_id < 0 || leader_node_id >= static_cast<int>(servers_.size()) || 
        !servers_[leader_node_id]) {
        response->set_success(false);
        response->set_leader_id(leader_node_id);
        std::cout << "Node " << me_ << ": Invalid leader node ID: " << leader_node_id << std::endl;
        return Status::CANCELLED;
    }
    
    raft::CommandRequest forwarded_request;
    forwarded_request.set_command(request->command());
    
    raft::CommandResponse leader_response;
    grpc::ClientContext context;
    
    // 设置RPC超时
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(3);
    context.set_deadline(deadline);
    
    grpc::Status status = servers_[leader_node_id]->SendCommand(&context, forwarded_request, &leader_response);
    
    if (status.ok()) {
        response->CopyFrom(leader_response);
        std::cout << "Node " << me_ << ": Forwarded command to leader " << leader_node_id << std::endl;
    } else {
        response->set_success(false);
        response->set_leader_id(leader_node_id);
        std::cout << "Node " << me_ << ": Failed to forward command to leader: " << status.error_message() << std::endl;
    }
    last_heartbeat_ = std::chrono::steady_clock::now(); // 重置选举超时
    return status;
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