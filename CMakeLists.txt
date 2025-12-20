cmake_minimum_required(VERSION 3.25)
project(MotherDuckFivetranDestination C CXX)

include(FetchContent)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options(-ftime-trace)

if(DEFINED ENV{MD_FIVETRAN_DEPENDENCIES_DIR})
    set(DEPENDENCIES_DIR "$ENV{MD_FIVETRAN_DEPENDENCIES_DIR}")
else()
    set(DEPENDENCIES_DIR "${CMAKE_CURRENT_LIST_DIR}/install" CACHE STRING "Path to dependencies")
endif()
message(STATUS "DEPENDENCIES DIR: ${DEPENDENCIES_DIR}")

foreach(dependency grpc openssl)
    if(NOT EXISTS ${DEPENDENCIES_DIR}/${dependency})
        message(FATAL_ERROR "Directory ${DEPENDENCIES_DIR}/${dependency} does not exist. Make sure to install the dependencies using `make build_dependencies` first.")
    endif()
endforeach()

find_package(Threads REQUIRED)

# absl and utf8_range are required by protobuf
find_package(absl CONFIG REQUIRED PATHS ${DEPENDENCIES_DIR}/grpc NO_DEFAULT_PATH)
find_package(utf8_range CONFIG REQUIRED PATHS ${DEPENDENCIES_DIR}/grpc NO_DEFAULT_PATH)
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
# If building in debug, set USE_SANITIZERS to ON
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(USE_SANITIZERS ON)
endif()

### DuckDB dependency ###
message(STATUS "Fetching DuckDB...")
set(FETCHCONTENT_TRY_FIND_PACKAGE_MODE NEVER)
set(FETCHCONTENT_QUIET OFF)
FetchContent_Declare(
    duckdb
    GIT_REPOSITORY https://github.com/duckdb/duckdb.git
    GIT_TAG v1.4.1
    GIT_SHALLOW TRUE
    SYSTEM
)

set(ENABLE_EXTENSION_AUTOINSTALL OFF)
# We need autoloading enabled to be able to connect to MotherDuck via md:
# TODO: Check that this is true
set(ENABLE_EXTENSION_AUTOLOADING ON)
set(BUILD_SHELL OFF)
set(BUILD_UNITTESTS OFF)
# core_functions is always built
set(BUILD_EXTENSIONS "parquet")

if (DEFINED USE_SANITIZERS AND USE_SANITIZERS)
    set(ENABLE_SANITIZER ON)
    set(ENABLE_UBSAN ON)
else()
    set(ENABLE_SANITIZER OFF)
    set(ENABLE_UBSAN OFF)
endif()

# Set build type to RELEASE for DuckDB because we use the release build of the MotherDuck extension
set(CMAKE_BUILD_TYPE_BACKUP ${CMAKE_BUILD_TYPE})
set(CMAKE_BUILD_TYPE Release)
FetchContent_MakeAvailable(duckdb)
# Restore build type
set(CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE_BACKUP})

get_target_property(DuckDB_INCLUDE_DIRS duckdb_static INTERFACE_INCLUDE_DIRECTORIES)

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

### MotherDuck destination connector ###
add_library(motherduck_destination_sources STATIC
        src/config_tester.cpp
        src/csv_processor.cpp
        src/decryption.cpp
        src/extension_helper.cpp
        src/fivetran_duckdb_interop.cpp
        src/memory_backed_file.cpp
        src/md_logging.cpp
        src/motherduck_destination_server.cpp
        src/openssl_helper.cpp
        src/schema_types.cpp
        src/sql_generator.cpp
        src/stacktrace.cpp
        src/temp_database.cpp
)

target_include_directories(motherduck_destination_sources PUBLIC
        includes
        PUBLIC SYSTEM
        ${DuckDB_INCLUDE_DIRS}
)

target_link_libraries(motherduck_destination_sources PUBLIC
        duckdb_static
        fivetran_sdk
        OpenSSL::Crypto
)

# The short commit SHA of the latest commit is used as version number in the custom_user_agent
target_compile_definitions(motherduck_destination_sources PRIVATE
    GIT_COMMIT_SHA="${GIT_COMMIT_SHA}"
)

if (DEFINED USE_SANITIZERS AND USE_SANITIZERS)
    target_compile_options(motherduck_destination_sources PUBLIC "-fsanitize=address,undefined")
    target_link_options(motherduck_destination_sources PUBLIC "-fsanitize=address,undefined")
endif()

target_compile_options(motherduck_destination_sources PUBLIC -Wall -Wextra -Wpedantic -Werror)

add_executable(motherduck_destination src/motherduck_destination.cpp)
target_link_libraries(
        motherduck_destination motherduck_destination_sources
)

# Testing
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/test)
    add_subdirectory(test)
endif()
