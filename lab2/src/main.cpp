// main.cpp
#include "raft.h"
#include "config.h"
#include <iostream>
#include <grpcpp/grpcpp.h>

int main(int argc, char** argv) {
    std::cout << "MIT 6.5840 Lab 2: Raft\n";
    
    // 示例：启动一个3节点的Raft集群
    Config cfg(3);
    cfg.Start();
    
    // 运行测试
    // testing::InitGoogleTest(&argc, argv);
    // return RUN_ALL_TESTS();
    
    return 0;
}