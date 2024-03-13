cmake_minimum_required(VERSION 3.8)
project(MotherDuckFivetranDestination C CXX)

set(TARGET_ROOT ${PROJECT_SOURCE_DIR}/gen)
include(./proto_helper.cmake)

find_package(Threads REQUIRED)

message(WARNING "DEPENDENCIES DIR: " ${MD_FIVETRAN_DEPENDENCIES_DIR})

# absl and utf8_range is required by protobuf
find_package(absl CONFIG REQUIRED PATHS ${MD_FIVETRAN_DEPENDENCIES_DIR}/grpc NO_DEFAULT_PATH)
find_package(utf8_range CONFIG REQUIRED PATHS ${MD_FIVETRAN_DEPENDENCIES_DIR}/grpc NO_DEFAULT_PATH)
find_package(Protobuf CONFIG REQUIRED PATHS ${MD_FIVETRAN_DEPENDENCIES_DIR}/grpc NO_DEFAULT_PATH)
find_package(Arrow REQUIRED PATHS ${MD_FIVETRAN_DEPENDENCIES_DIR}/arrow NO_DEFAULT_PATH)

message(STATUS "Using arrow ${ARROW_VERSION}")


set(Protobuf_INCLUDE_DIR "${MD_FIVETRAN_DEPENDENCIES_DIR}/grpc/include")
set(Protobuf_LIBRARIES "${MD_FIVETRAN_DEPENDENCIES_DIR}/grpc/lib")
set(Openssl_INCLUDE_DIR "${MD_FIVETRAN_DEPENDENCIES_DIR}/openssl/include")
set(Openssl_LIBRARIES "${MD_FIVETRAN_DEPENDENCIES_DIR}/openssl/lib")

message(STATUS "Using protobuf ${Protobuf_VERSION}")

find_package(gRPC CONFIG REQUIRED PATHS ${MD_FIVETRAN_DEPENDENCIES_DIR}/grpc NO_DEFAULT_PATH)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
if(CMAKE_CROSSCOMPILING)
  find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
  set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()


# Fivetran proto files
generate_protos(${CMAKE_CURRENT_SOURCE_DIR}/protos/common.proto)
generate_protos(${CMAKE_CURRENT_SOURCE_DIR}/protos/destination_sdk.proto)

# Motherduck logging proto
generate_protos(${CMAKE_CURRENT_SOURCE_DIR}/protos/logging_sink.proto)

# DuckDB amalgamation
set(DuckDB_SOURCE_DIR ./libduckdb-src)


add_library(motherduck_destination_sources STATIC
        "src/motherduck_destination_server.cpp"
        "src/sql_generator.cpp"
        "src/decryption.cpp"
        "src/csv_arrow_ingest.cpp"
        "src/fivetran_duckdb_interop.cpp"
        "src/md_logging.cpp"
        ${TARGET_ROOT}/cpp/logging_sink.pb.cc
        ${TARGET_ROOT}/cpp/logging_sink.grpc.pb.cc
        ${TARGET_ROOT}/cpp/destination_sdk.pb.cc
        ${TARGET_ROOT}/cpp/destination_sdk.grpc.pb.cc
        ${TARGET_ROOT}/cpp/common.pb.cc
        ${TARGET_ROOT}/cpp/common.grpc.pb.cc
        ${DuckDB_SOURCE_DIR}/duckdb.cpp
)

target_include_directories(motherduck_destination_sources PUBLIC
        ${TARGET_ROOT}/cpp
        includes
        ${DuckDB_SOURCE_DIR}
        ${Openssl_INCLUDE_DIR}/include/openssl
        ${DUCKDB_INCLUDES}
)

link_dependencies(motherduck_destination_sources)

target_link_libraries(motherduck_destination_sources
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        Arrow::arrow_static
)

add_executable(motherduck_destination "src/motherduck_destination.cpp")
target_link_libraries(
        motherduck_destination motherduck_destination_sources
)

# Testing
find_package(Catch2 3 REQUIRED PATHS ${MD_FIVETRAN_DEPENDENCIES_DIR}/Catch2 NO_DEFAULT_PATH)
add_executable(integration_tests
        test/integration/test_server.cpp
)
target_compile_definitions(integration_tests PRIVATE
        TEST_RESOURCES_LOCATION=${CMAKE_CURRENT_LIST_DIR}/test/files/
)

target_include_directories(integration_tests PUBLIC
        ${TARGET_ROOT}/cpp
        includes
        ${Protobuf_INCLUDE_DIR}
        ${DUCKDB_INCLUDES}

)
target_link_libraries(integration_tests PRIVATE
        Catch2::Catch2WithMain
        motherduck_destination_sources
)
