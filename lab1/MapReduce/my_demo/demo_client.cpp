#include <iostream>
#include <memory>
#include <string>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "include/demo.pb.h"
#include "include/demo.grpc.pb.h"

class Client
{
public:
    Client(std::shared_ptr<grpc::ChannelInterface> channel) : stub_(demo::Master::NewStub(channel)) {}
    
    void showhellow() {
        grpc::ClientContext context;
        demo::Empty request;
        demo::Response response;
        grpc::Status status = stub_->showhellow(&context, request, &response);
        if (status.ok()) {
            std::cout << "Received: " << response.message() << std::endl;
        }
        else {
            std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        }
    }
    
private:
    std::unique_ptr<demo::Master::Stub> stub_;
};

int main()
{
    Client client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
    client.showhellow();
    return 0;
}