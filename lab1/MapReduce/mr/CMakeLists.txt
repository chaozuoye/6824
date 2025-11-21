cmake_minimum_required(VERSION 3.14)
project(map_reduce)
# 启用 C++14 或更高版本
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

# 查找 Protobuf 和 gRPC
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# 获取必要的包含目录
get_target_property(gRPC_INCLUDE_DIRS gRPC::grpc++ INTERFACE_INCLUDE_DIRECTORIES)
include_directories(${gRPC_INCLUDE_DIRS})

# 设置变量
set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_GRPC_GRPCPP gRPC::grpc++)
set(_REFLECTION gRPC::grpc++_reflection)

# Proto 文件定义
set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/map_reduce.proto")
set(GENERATED_PROTOBUF_PATH "${CMAKE_CURRENT_SOURCE_DIR}/include/")

# 确保生成目录存在
file(MAKE_DIRECTORY ${GENERATED_PROTOBUF_PATH})

# 生成 Protobuf 和 gRPC 文件
add_custom_command(
    OUTPUT 
        "${GENERATED_PROTOBUF_PATH}/map_reduce.pb.cc"
        "${GENERATED_PROTOBUF_PATH}/map_reduce.pb.h"
        "${GENERATED_PROTOBUF_PATH}/map_reduce.grpc.pb.cc"
        "${GENERATED_PROTOBUF_PATH}/map_reduce.grpc.pb.h"
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
    "${GENERATED_PROTOBUF_PATH}/map_reduce.pb.cc"
    "${GENERATED_PROTOBUF_PATH}/map_reduce.pb.h"
    "${GENERATED_PROTOBUF_PATH}/map_reduce.grpc.pb.cc"
    "${GENERATED_PROTOBUF_PATH}/map_reduce.grpc.pb.h"
)

include_directories(${GENERATED_PROTOBUF_PATH})

set(hw_proto_srcs 
    "${GENERATED_PROTOBUF_PATH}/map_reduce.pb.cc"
    "${GENERATED_PROTOBUF_PATH}/map_reduce.grpc.pb.cc"
)

add_library(hw_grpc_proto ${hw_proto_srcs})
add_dependencies(hw_grpc_proto GenerateProtoFiles)


add_library(tools SHARED tools.cpp)

set_target_properties(tools PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(hw_grpc_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
)

target_link_libraries(tools
    ${CMAKE_DL_LIBS}
)


foreach(_target mr_master mr_worker)
    add_executable(${_target} "${_target}.cpp")
    target_link_libraries(${_target}
        hw_grpc_proto
        tools  
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        ${CMAKE_DL_LIBS}
    )

    add_custom_command(TARGET ${_target} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        $<TARGET_FILE:tools>
        $<TARGET_FILE_DIR:${_target}>
    )
endforeach()