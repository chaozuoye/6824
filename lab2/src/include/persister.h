#pragma once

#include <string>
#include <memory>
#include <mutex>

class Persister {
public:
  static std::shared_ptr<Persister> MakePersister();
  
  void Save(const std::string& raft_state, const std::string& snapshot);
  std::string ReadRaftState();
  std::string ReadSnapshot();
  int RaftStateSize();
  
private:
  mutable std::mutex mutex_;
  std::string raft_state_;
  std::string snapshot_;
};