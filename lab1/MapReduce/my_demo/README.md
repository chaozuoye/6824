# 学习grpc的一个demo

要使用grpc首先需要定义消息体，定义消息体就需要用到protobuf
本人使用vcpkg安装protobuf遇到很多坑导致后面cmake无法识别protobuf和grpc
使用vcpkg install protobuf和grpc后记得使用vcpkg integrate install命令让cmake识别protobuf和grpc

还有protoc-gen-grpc插件路径在vcpkg的安装目录下,使用protoc命令生成grpc代码时如果找不到插件路径需要自己手动指定路径，或者自己设置环境变量（太坑了）
以下是我本人的配置

    protoc .\demo.proto --cpp_out=./include --grpc_out=./include --plugin=protoc-gen-grpc=E:/vcpkg/vcpkg/packages/grpc_x64-windows/tools/grpc/grpc_cpp_plugin.exe 

编译：

    cd build
    cmake .. 
    cd Debug
    MSBuild.exe demo.sln

程序很简单，就是server服务器提供一个showhellow 函数，client调用这个函数打印hello world