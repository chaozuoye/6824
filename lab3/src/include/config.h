#pragma once

#include <vector>
#include <memory>
#include <thread>
#include "raft.h"
#include "persister.h"

class Config {
public:
  Config(int n, bool unreliable = false);
  ~Config();
  
  void start1(int i, std::function<void(int, const raft::ApplyMsg&)> applier);
  void connect(int i);
  void disconnect(int i);
  void crash1(int i);
  int check_one_leader();
  void one(const std::string& cmd, int expected_servers, bool retry);
  
private:
  int n_;
  std::vector<std::shared_ptr<Raft>> rafts_;
  std::vector<std::shared_ptr<Persister>> persisters_;
  std::vector<bool> connected_;
  std::vector<std::map<int, std::string>> logs_;
  std::vector<std::string> apply_err_;
};