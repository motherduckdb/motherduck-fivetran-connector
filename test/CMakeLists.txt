cmake_minimum_required(VERSION 3.20)

if(NOT EXISTS ${DEPENDENCIES_DIR}/catch2)
    message(FATAL_ERROR "Directory ${DEPENDENCIES_DIR}/catch2 does not exist. Make sure to install the dependencies using `make build_test_dependencies` first.")
endif()

find_package(Catch2 3 REQUIRED PATHS ${DEPENDENCIES_DIR}/catch2 NO_DEFAULT_PATH)

add_executable(integration_tests
        test_decryption.cpp
        integration/test_server.cpp
)
target_compile_definitions(integration_tests PRIVATE
        TEST_RESOURCES_LOCATION=${CMAKE_CURRENT_LIST_DIR}/files/
)
target_link_libraries(integration_tests PRIVATE
        Catch2::Catch2WithMain
        motherduck_destination_sources
)

# Place executable in the root build directory
set_target_properties(integration_tests PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
)