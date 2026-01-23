// client.cpp
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>
#include "include/raft.grpc.pb.h"
#include <nlohmann/json.hpp>
#include <fstream>
using json = nlohmann::json;

class RaftClient {
public:
    RaftClient(const std::vector<std::string>& server_addresses) {
        for (const auto& addr : server_addresses) {
            auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
            auto stub = raft::RaftService::NewStub(channel);
            stubs_.push_back(std::move(stub));
        }
    }

    void run() {
        std::cout << "Raft Cluster Client\n";
        std::cout << "Available commands: SET key value, GET key, DELETE key, STATUS, PING, QUIT\n";
        std::cout << "Type 'help' for more information.\n\n";

        std::string input;
        while (true) {
            std::cout << "> ";
            std::getline(std::cin, input);
            
            if (input.empty()) continue;
            
            // 解析命令
            std::istringstream iss(input);
            std::string command;
            iss >> command;
            
            std::transform(command.begin(), command.end(), command.begin(), ::toupper);
            
            if (command == "QUIT" || command == "EXIT") {
                break;
            } else if (command == "HELP") {
                showHelp();
            } else if (command == "STATUS") {
                getStatus();
            } else if (command == "PING") {
                pingAllServers();
            // } else if (command == "SET" || command == "GET" || command == "DELETE") {
            //     processKeyValueCommand(input);
            } else {
                std::cout << "Unknown command. Type 'help' for available commands.\n";
            }
        }
    }

private:
    std::vector<std::unique_ptr<raft::RaftService::Stub>> stubs_;

    void showHelp() {
        std::cout << "\nAvailable commands:\n";
        std::cout << "  SET <key> <value>    - Set a key-value pair\n";
        std::cout << "  GET <key>            - Get value for a key\n";
        std::cout << "  DELETE <key>         - Delete a key\n";
        std::cout << "  STATUS               - Show cluster status\n";
        std::cout << "  PING                 - Ping all servers\n";
        std::cout << "  QUIT/EXIT            - Exit the client\n";
        std::cout << "  HELP                 - Show this help\n\n";
    }

    // void processKeyValueCommand(const std::string& input) {
    //     // 发送命令到集群
    //     int response_index, response_term;
    //     bool success;
        
    //     if (sendCommandToCluster(input, response_index, response_term, success)) {
    //         if (success) {
    //             std::cout << "Command executed successfully!\n";
    //             std::cout << "Log index: " << response_index << std::endl;
    //             std::cout << "Term: " << response_term << std::endl;
    //         } else {
    //             std::cout << "Command failed - node is not the leader\n";
    //         }
    //     } else {
    //         std::cout << "Failed to execute command on any server\n";
    //     }
    // }

    // bool sendCommandToCluster(const std::string& command, int& response_index, int& response_term, bool& success) {
    //     for (size_t i = 0; i < stubs_.size(); ++i) {
    //         raft::CommandRequest request;
    //         request.set_command(command);
            
    //         raft::CommandResponse response;
    //         grpc::ClientContext context;
            
    //         auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
    //         context.set_deadline(deadline);
            
    //         grpc::Status status = stubs_[i]->SendCommand(&context, request, &response);
            
    //         if (status.ok()) {
    //             if (response.success()) {
    //                 response_index = response.index();
    //                 response_term = response.term();
    //                 success = true;
    //                 return true;
    //             }
    //             // 如果不成功但有领导者信息，可以尝试连接领导者
    //             else if (response.has_leader_id() && response.leader_id() != -1) {
    //                 std::cout << "Node suggests leader: " << response.leader_id() << std::endl;
    //             }
    //         } else {
    //             std::cout << "Server " << i << " error: " << status.error_message() << std::endl;
    //         }
    //     }
        
    //     success = false;
    //     return false;
    // }

    void getStatus() {
        std::cout << "\nCluster Status:\n";
        for (size_t i = 0; i < stubs_.size(); ++i) {
            raft::CommandRequest request;
            request.set_command("STATUS_QUERY");  // 假设有个状态查询命令
            
            raft::CommandResponse response;
            grpc::ClientContext context;
            
            auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
            context.set_deadline(deadline);
            
            grpc::Status status = stubs_[i]->SendCommand(&context, request, &response);
            
            if (status.ok()) {
                std::cout << "  Server " << i << ": ACTIVE";
                std::cout << " (Leader: " << response.leader_id() << ")";
                std::cout << std::endl;
            } else {
                std::cout << "  Server " << i << ": INACTIVE (" << status.error_message() << ")" << std::endl;
            }
        }
        std::cout << std::endl;
    }

    void pingAllServers() {
        std::cout << "\nPing Results:\n";
        for (size_t i = 0; i < stubs_.size(); ++i) {
            raft::AppendEntriesRequest request;
            request.set_term(0);
            request.set_leader_id(-1);
            request.set_prev_log_index(-1);
            request.set_prev_log_term(0);
            request.set_leader_commit(0);
            
            raft::AppendEntriesResponse response;
            grpc::ClientContext context;
            
            auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(500);
            context.set_deadline(deadline);
            
            grpc::Status status = stubs_[i]->AppendEntries(&context, request, &response);
            
            std::cout << "  Server " << i << ": " << (status.ok() ? "OK" : "FAILED") << std::endl;
        }
        std::cout << std::endl;
    }
};
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
int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }
    std::string configFile = argv[1];
    // 集群节点地址
    std::vector<std::string> server_addresses = readNodeConfig(configFile);
    if(server_addresses.empty()) {
        std::cerr << "No server addresses found in config file." << std::endl;
        return 1;
    }
    RaftClient client(server_addresses);
    client.run();

    return 0;
}