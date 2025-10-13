cmake_minimum_required(VERSION 3.20)

find_package(Catch2 3 REQUIRED PATHS ${DEPENDENCIES_DIR}/Catch2 NO_DEFAULT_PATH)

add_executable(integration_tests
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