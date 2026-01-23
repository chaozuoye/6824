cmake_minimum_required(VERSION 3.14)
project(raft)

set(CMAKE_CXX_STANDARD 17)

# 查找必要的包
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# 查找nlohmann_json
find_package(nlohmann_json QUIET)

# 如果没有找到nlohmann_json，使用FetchContent下载
if(NOT nlohmann_json_FOUND)
    message(STATUS "nlohmann/json not found, will use FetchContent to download it")
    include(FetchContent)
    FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG v3.11.2
    )
    FetchContent_MakeAvailable(nlohmann_json)
endif()

# 手动指定proto文件
set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/raft.proto")
set(GENERATED_PROTOBUF_PATH "${CMAKE_CURRENT_SOURCE_DIR}/include/")

# 生成 Protobuf 和 gRPC 文件
add_custom_command(
    OUTPUT
        "${GENERATED_PROTOBUF_PATH}/raft.pb.cc"
        "${GENERATED_PROTOBUF_PATH}/raft.pb.h"
        "${GENERATED_PROTOBUF_PATH}/raft.grpc.pb.cc"
        "${GENERATED_PROTOBUF_PATH}/raft.grpc.pb.h"
    COMMAND protobuf::protoc
    ARGS
        --proto_path "${CMAKE_CURRENT_SOURCE_DIR}"
        --cpp_out "${GENERATED_PROTOBUF_PATH}"
        --grpc_out "${GENERATED_PROTOBUF_PATH}"
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating C++ protocol buffer and gRPC sources from ${PROTO_FILE}"
    VERBATIM
)
add_custom_target(GenerateProtoFiles ALL DEPENDS
    "${GENERATED_PROTOBUF_PATH}/raft.pb.cc"
    "${GENERATED_PROTOBUF_PATH}/raft.pb.h"
    "${GENERATED_PROTOBUF_PATH}/raft.grpc.pb.cc"
    "${GENERATED_PROTOBUF_PATH}/raft.grpc.pb.h"
)
include_directories(${GENERATED_PROTOBUF_PATH})
# 设置生成的源文件
set(PROTO_SRCS 
    "${GENERATED_PROTOBUF_PATH}/raft.pb.cc"
    "${GENERATED_PROTOBUF_PATH}/raft.grpc.pb.cc"
)

set(PROTO_HDRS
    "${GENERATED_PROTOBUF_PATH}/raft.pb.h"
    "${GENERATED_PROTOBUF_PATH}/raft.grpc.pb.h"
)

# 创建可执行文件
add_executable(raft
    raft.cpp
    ${PROTO_SRCS}
)

# 链接库
target_link_libraries(raft
    gRPC::grpc++
    gRPC::grpc
    protobuf::libprotobuf
)

# 包含目录
target_include_directories(raft
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}
)

# 如果通过FetchContent获取了nlohmann_json，则链接它
if(TARGET nlohmann_json::nlohmann_json)
    target_link_libraries(raft nlohmann_json::nlohmann_json)
endif()

# 创建客户端可执行文件
add_executable(raft_client
    client.cpp
    ${PROTO_SRCS}
)

# 链接库
target_link_libraries(raft_client
    gRPC::grpc++
    gRPC::grpc
    protobuf::libprotobuf
)

# 包含目录
target_include_directories(raft_client
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}
)

# 如果通过FetchContent获取了nlohmann_json，则链接它
if(TARGET nlohmann_json::nlohmann_json)
    target_link_libraries(raft_client nlohmann_json::nlohmann_json)
endif()