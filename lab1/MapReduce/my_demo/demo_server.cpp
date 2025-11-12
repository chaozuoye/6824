#include <iostream>
#include <memory>
#include <string>
#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>
#include "include/demo.pb.h"
#include "include/demo.grpc.pb.h"

// 使用正确的命名空间
class MasterImpl final : public demo::Master::Service {
public:
    grpc::Status showhellow(grpc::ServerContext* context, const demo::Empty* request, demo::Response* reply) override {
        reply->set_message("hello world");
        return grpc::Status::OK;
    }
};

int main(int argc, char** argv) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());
    auto service = std::make_unique<MasterImpl>();
    builder.RegisterService(service.get());
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on 0.0.0.0:50051" << std::endl;
    server->Wait();
    return 0;
}