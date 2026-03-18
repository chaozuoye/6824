#include "include/raft.h"
#include <iostream>
int main(int argc, char** argv) {
    if(argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <node_id> <node.json>" << std::endl;
        return 1;
    }
    
    try {
        auto raftNode = std::make_unique<Raft>(atoi(argv[1]), argv[2]);
        raftNode->Run();
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}