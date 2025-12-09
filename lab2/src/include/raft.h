#pragma once

#include <vector>
#include <memory>
#include <mutex>
#include <thread>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"
using namespace raft;
// #include "persister.h"

// 日志条目结构体，表示 Raft 日志中的单个条目
// struct LogEntry {
//   int term;           // 条目所在的任期
//   std::string command; // 状态机命令
//   int index;          // 条目在日志中的索引
// };

// Raft 类实现 Raft 一致性算法
class Raft : public raft::RaftService::Service {
public:
  // 构造函数
  Raft(int me, const std::string& configFile);
  
  ~Raft();

  // 对外接口
  std::tuple<int, int, bool> Start(const std::string& command);  // 客户端启动一个新的日志条目
  std::pair<int, bool> GetState();  // 获取当前节点的状态（任期和是否为领导者）
  void Kill();                      // 关闭该 Raft 节点
  void startElection();             // 发起选举
  void Run();                       // 运行 Raft 节点的主循环

  // RPC handlers
  grpc::Status RequestVote(grpc::ServerContext* context,
                          const raft::RequestVoteRequest* request,
                          raft::RequestVoteResponse* response) override;
                          
  grpc::Status AppendEntries(grpc::ServerContext* context,
                            const raft::AppendEntriesRequest* request,
                            raft::AppendEntriesResponse* response) override;
                            
  grpc::Status InstallSnapshot(grpc::ServerContext* context,
                              const raft::InstallSnapshotRequest* request,
                              raft::InstallSnapshotResponse* response) override;

private:
  mutable std::mutex mutex_;                           // 保护共享数据的互斥锁
  std::vector<std::unique_ptr<raft::RaftService::Stub>> servers_;  // 指向其他节点的 gRPC 存根
//   std::shared_ptr<Persister> persister_;               // 持久化存储对象
  int me_;                                             // 当前节点的索引
  std::atomic<bool> dead_{false};                      // 节点是否已关闭
  
  // Raft 状态
  raft::Role role_;           // 当前节点的角色（跟随者、候选者、领导者）
  int current_term_;      // 当前任期
  int voted_for_;         // 当前任期投票给了谁
  std::vector<LogEntry> log_;      // 日志条目
  int commit_index_;      // 已知已提交的最高日志条目的索引
  int last_applied_;      // 已应用到状态机的最高日志条目的索引
  std::vector<std::string> peerAddresses; // 集群中所有节点的地址
  
  // 领导者特有状态
  std::vector<int> next_index_;   // 对于每个服务器，下一个要发送给它的日志条目的索引
  std::vector<int> match_index_;  // 对于每个服务器，已知的最高匹配的日志条目索引
  
  // 时间控制
  std::chrono::steady_clock::time_point last_heartbeat_;  // 上次收到心跳的时间
  std::chrono::milliseconds election_timeout_;            // 选举超时时间
  
//   std::function<void(const raft::ApplyMsg&)> apply_ch_;   // 应用日志条目的通道
  std::thread ticker_thread_;                             // 定时器线程

  // 内部方法
  void ticker();                  // 定时检查是否需要发起选举
  void start_election();          // 开始选举
  void become_follower(int term); // 转变为跟随者
  void become_candidate();        // 转变为候选者
  void become_leader();           // 转变为领导者
  void persist();                 // 持久化状态
  void read_persist();            // 从持久化存储中读取状态
  void apply_logs();              // 应用已提交的日志到状态机
  
  // RPC 辅助方法
  bool send_request_vote(int server, const raft::RequestVoteRequest& req, 
                        raft::RequestVoteResponse* resp);
  bool send_append_entries(int server, const raft::AppendEntriesRequest& req,
                          raft::AppendEntriesResponse* resp);
  bool send_install_snapshot(int server, const raft::InstallSnapshotRequest& req,
                            raft::InstallSnapshotResponse* resp);
};