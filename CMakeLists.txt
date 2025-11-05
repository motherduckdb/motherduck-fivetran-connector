cmake_minimum_required(VERSION 3.10...3.20)
project(MotherDuckFivetranDestination C CXX)

# Debug Asan/Ubsan
if(CMAKE_BUILD_TYPE STREQUAL "Debug" AND NOT DISABLE_SANITIZER)
  if("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang$")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize-blacklist=${CMAKE_CURRENT_LIST_DIR}/sanitizer-disallowed-entries.txt")
    set(ENABLE_UBSAN TRUE)
  else()
    set(ENABLE_UBSAN FALSE)
  endif()

  add_compile_options("-fsanitize=address,undefined")
  add_link_options("-fsanitize=address,undefined")
  set(ENABLE_SANITIZER TRUE)
else()
  set(ENABLE_SANITIZER FALSE)
  set(ENABLE_UBSAN FALSE)
endif()

message("-- ENABLE_SANITIZER=${ENABLE_SANITIZER}")
message("-- ENABLE_UBSAN=${ENABLE_UBSAN}")

if(DEFINED ENV{MD_FIVETRAN_DEPENDENCIES_DIR})
    set(DEPENDENCIES_DIR "$ENV{MD_FIVETRAN_DEPENDENCIES_DIR}")
else()
    set(DEPENDENCIES_DIR "${CMAKE_CURRENT_LIST_DIR}/install" CACHE STRING "Path to dependencies")
endif()
message(STATUS "DEPENDENCIES DIR: ${DEPENDENCIES_DIR}")

foreach(dependency grpc arrow openssl)
    if(NOT EXISTS ${DEPENDENCIES_DIR}/${dependency})
        message(FATAL_ERROR "Directory ${DEPENDENCIES_DIR}/${dependency} does not exist. Make sure to install the dependencies using `make build_dependencies` first.")
    endif()
endforeach()

find_package(Threads REQUIRED)

# absl and utf8_range are required by protobuf
find_package(absl CONFIG REQUIRED PATHS ${DEPENDENCIES_DIR}/grpc NO_DEFAULT_PATH)
find_package(utf8_range CONFIG REQUIRED PATHS ${DEPENDENCIES_DIR}/grpc NO_DEFAULT_PATH)
find_package(Arrow REQUIRED PATHS ${DEPENDENCIES_DIR}/arrow NO_DEFAULT_PATH)
message(STATUS "Using arrow ${ARROW_VERSION}")
find_package(Protobuf CONFIG REQUIRED PATHS ${DEPENDENCIES_DIR}/grpc NO_DEFAULT_PATH)
message(STATUS "Using protobuf ${Protobuf_VERSION}")
set(Protobuf_INCLUDE_DIR "${DEPENDENCIES_DIR}/grpc/include")

set(OPENSSL_ROOT_DIR "${DEPENDENCIES_DIR}/openssl")
find_package(OpenSSL MODULE REQUIRED)
message(STATUS "Using OpenSSL ${OPENSSL_VERSION}, include dir: ${OPENSSL_INCLUDE_DIR}")

find_package(gRPC CONFIG REQUIRED PATHS ${DEPENDENCIES_DIR}/grpc NO_DEFAULT_PATH)
message(STATUS "Using gRPC ${gRPC_VERSION}")

add_link_options(-rdynamic)
add_compile_options(-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer)

### Store latest git commit SHA ###
if(DEFINED GIT_COMMIT_SHA_OVERRIDE AND NOT GIT_COMMIT_SHA_OVERRIDE STREQUAL "")
    set(GIT_COMMIT_SHA ${GIT_COMMIT_SHA_OVERRIDE})
    message(STATUS "Using overridden Git commit SHA: ${GIT_COMMIT_SHA}")
else()
    find_package(Git REQUIRED)
    if(NOT GIT_FOUND)
        message(FATAL_ERROR "Git executable not found. Make sure that git is installed or provide GIT_COMMIT_SHA_OVERRIDE.")
    endif()

    execute_process(
        COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_COMMIT_SHA
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE GIT_RESULT
        ERROR_VARIABLE GIT_ERROR
    )

    if(NOT GIT_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to get git commit SHA: ${GIT_ERROR}")
    endif()

    if(NOT GIT_COMMIT_SHA)
        message(FATAL_ERROR "Git commit SHA is empty. Make sure the git repository is initialized correctly.")
    endif()

    message(STATUS "Git commit SHA: ${GIT_COMMIT_SHA}")
endif()

### Proto generation ###
set(FIVETRAN_SDK ${PROJECT_SOURCE_DIR}/gen)

if(CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

# Loads the generate_protos and link_dependencies functions
set(TARGET_ROOT ${FIVETRAN_SDK})
include(./proto_helper.cmake)

# Proto files
generate_protos(${CMAKE_CURRENT_SOURCE_DIR}/protos/common.proto)
generate_protos(${CMAKE_CURRENT_SOURCE_DIR}/protos/destination_sdk.proto)

add_library(fivetran_sdk OBJECT
        ${FIVETRAN_SDK}/cpp/destination_sdk.pb.cc
        ${FIVETRAN_SDK}/cpp/destination_sdk.grpc.pb.cc
        ${FIVETRAN_SDK}/cpp/common.pb.cc
        ${FIVETRAN_SDK}/cpp/common.grpc.pb.cc
)
target_include_directories(fivetran_sdk PUBLIC ${FIVETRAN_SDK}/cpp)
link_dependencies(fivetran_sdk)

### DuckDB ###
find_library(DUCKDB_LIB NAMES duckdb REQUIRED PATHS ${CMAKE_SOURCE_DIR}/libduckdb NO_DEFAULT_PATH)

### MotherDuck destination connector ###
add_library(motherduck_destination_sources STATIC
        src/csv_arrow_ingest.cpp
        src/decryption.cpp
        src/extension_helper.cpp
        src/fivetran_duckdb_interop.cpp
        src/md_logging.cpp
        src/motherduck_destination_server.cpp
        src/openssl_helper.cpp
        src/sql_generator.cpp
        src/stacktrace.cpp
)

target_include_directories(motherduck_destination_sources PUBLIC
        includes
        ${CMAKE_SOURCE_DIR}/libduckdb
)

target_link_libraries(motherduck_destination_sources PUBLIC
        Arrow::arrow_static
        ${DUCKDB_LIB}
        fivetran_sdk
        OpenSSL::Crypto
)

# The short commit SHA of the latest commit is used as version number in the custom_user_agent
target_compile_definitions(motherduck_destination_sources PRIVATE
    GIT_COMMIT_SHA="${GIT_COMMIT_SHA}"
)

add_executable(motherduck_destination src/motherduck_destination.cpp)
target_link_libraries(
        motherduck_destination motherduck_destination_sources
)

# Testing
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/test)
    add_subdirectory(test)
endif()
