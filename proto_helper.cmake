if (APPLE)
    set(APPLE_DEPENDENCIES "-framework CoreFoundation -lresolv")
else()
    if (UNIX AND NOT WIN32)
        set(LINUX_DEPENDENCIES rt)
    endif()
endif()

# https://jaredrhodes.com/2018/08/24/generate-protocol-buffers-on-build-with-cmake/
function(GENERATE_PROTOS PROTO_FILE_IN)
    if(WIN32)
        set(PROTOC ${DEPENDENCIES_DIR}/grpc/bin/protoc.exe)
        set(PROTOC_GEN_GRPC ${DEPENDENCIES_DIR}/grpc/bin/grpc_cpp_plugin.exe)
    else()
        set(PROTOC ${DEPENDENCIES_DIR}/grpc/bin/protoc)
        set(PROTOC_GEN_GRPC ${DEPENDENCIES_DIR}/grpc/bin/grpc_cpp_plugin)
    endif()

    if(NOT EXISTS ${PROTOC})
        message(FATAL_ERROR "protoc not found at " ${PROTOC})
    endif()

    if(NOT EXISTS ${PROTOC_GEN_GRPC})
        message(FATAL_ERROR "grpc_cpp_plugin not found at " ${PROTOC_GEN_GRPC})
    endif()

    message(DEBUG PROJECT_SOURCE_DIR= ${PROJECT_SOURCE_DIR})
    message(DEBUG TARGET_ROOT= ${TARGET_ROOT})

    # Create a tmp directory so that we can only overwrite proto files that have been updated
    string(RANDOM TMP_DIRECTORY)

    file(MAKE_DIRECTORY ${TARGET_ROOT}/${TMP_DIRECTORY})
    file(MAKE_DIRECTORY ${TARGET_ROOT}/cpp)

    message(STATUS "Generating proto files:")

    if(MOCK)
        set(MOCK_FLAG "generate_mock_code=true:")
    endif()
    execute_process(
            COMMAND
            ${PROTOC}
            --proto_path=${PROJECT_SOURCE_DIR}/protos
            --plugin=protoc-gen-grpc_cpp=${PROTOC_GEN_GRPC}
            --grpc_cpp_out=${MOCK_FLAG}${TARGET_ROOT}/${TMP_DIRECTORY}
            --cpp_out=${TARGET_ROOT}/${TMP_DIRECTORY}
            ${PROTO_FILE_IN}
            COMMAND_ECHO STDERR
    )

    file(GLOB SOURCE_FILES ${TARGET_ROOT}/${TMP_DIRECTORY}/*)
    foreach(SOURCE_FILE_FULL_PATH ${SOURCE_FILES})
        get_filename_component(SOURCE_FILE ${SOURCE_FILE_FULL_PATH} NAME)
        execute_process( COMMAND
                cmake -E compare_files ${SOURCE_FILE_FULL_PATH} ${TARGET_ROOT}/cpp/${SOURCE_FILE}
                RESULT_VARIABLE COMPARE_RESULT
        )
        if ( ${COMPARE_RESULT} EQUAL 1)
            message("Update detected in " ${SOURCE_FILE} " - copying to ${TARGET_ROOT}/cpp/")
            file(COPY ${SOURCE_FILE_FULL_PATH} DESTINATION ${TARGET_ROOT}/cpp/)
        endif()
    endforeach()

    execute_process(COMMAND rm -r ${TARGET_ROOT}/${TMP_DIRECTORY})
endfunction()

function(LINK_DEPENDENCIES LIBRARY_NAME)
    target_include_directories(${LIBRARY_NAME} PUBLIC SYSTEM
            ${TARGET_ROOT}/cpp/
            ${DEPENDENCIES_DIR}/grpc/include/
    )

    target_link_libraries(${LIBRARY_NAME}
            protobuf::libprotobuf
            gRPC::grpc
            gRPC::grpc++
            ${APPLE_DEPENDENCIES}
            ${LINUX_DEPENDENCIES}
    )

    # ASAN disabled, as it breaks gRPC, see https://github.com/grpc/grpc/issues/32433
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        target_compile_options(${LIBRARY_NAME} PRIVATE "-fno-sanitize=address")
    endif()
endfunction()
