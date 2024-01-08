#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include "motherduck_destination_server.hpp"

TEST_CASE("ConfigurationForm", "[integration]") {
    DestinationSdkImpl service;

    auto request = ::fivetran_sdk::ConfigurationFormRequest().New();
    auto response = ::fivetran_sdk::ConfigurationFormResponse().New();

    auto status = service.ConfigurationForm(nullptr, request, response);
    INFO(status.error_message());
    REQUIRE(status.ok());

    REQUIRE(response->fields_size() == 2);
    REQUIRE(response->fields(0).name() == "motherduck_token");
    REQUIRE(response->fields(1).name() == "motherduck_database");
}

TEST_CASE("DescribeTable fails when database missing", "[integration]") {
    DestinationSdkImpl service;

    ::fivetran_sdk::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = "12345";

    ::fivetran_sdk::DescribeTableResponse response;

    auto status = service.DescribeTable(nullptr, &request, &response);

    REQUIRE(!status.ok());
    REQUIRE(status.error_message() == "Missing property motherduck_database");
}

TEST_CASE("DescribeTable on nonexistent table", "[integration]") {
    DestinationSdkImpl service;

    ::fivetran_sdk::DescribeTableRequest request;
    auto token = std::getenv("motherduck_token");
    REQUIRE(token);
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.set_table_name("nonexistent_table");
    ::fivetran_sdk::DescribeTableResponse response;

    auto status = service.DescribeTable(nullptr, &request, &response);

    INFO(status.error_message());
    REQUIRE(status.ok());
    REQUIRE(response.not_found());
}

TEST_CASE("CreateTable", "[integration]") {
    DestinationSdkImpl service;

    const std::string schema_name = "some_schema" + std::to_string(Catch::rngSeed());
    const std::string table_name = "some_table" + std::to_string(Catch::rngSeed());
    {
        // Create Table
        ::fivetran_sdk::CreateTableRequest request;
        auto token = std::getenv("motherduck_token");
        REQUIRE(token);
        (*request.mutable_configuration())["motherduck_token"] = token;
        (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
        request.set_schema_name(schema_name);
        request.mutable_table()->set_name(table_name);
        ::fivetran_sdk::Column col1;
        col1.set_name("id");
        col1.set_type(::fivetran_sdk::DataType::STRING);
        request.mutable_table()->add_columns()->CopyFrom(col1);

        ::fivetran_sdk::CreateTableResponse response;
        auto status = service.CreateTable(nullptr, &request, &response);

        INFO(status.error_message());
        REQUIRE(status.ok());
    }

    {
        // Describe the created table
        ::fivetran_sdk::DescribeTableRequest request;
        auto token = std::getenv("motherduck_token");
        REQUIRE(token);
        (*request.mutable_configuration())["motherduck_token"] = token;
        (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
        request.set_table_name(table_name);

        {
            // table not found in default "main" schema
            ::fivetran_sdk::DescribeTableResponse response;
            auto status = service.DescribeTable(nullptr, &request, &response);

            INFO(status.error_message());
            REQUIRE(status.ok());
            REQUIRE(response.not_found());
        }

        {
            // table found in the correct schema
            request.set_schema_name(schema_name);
            ::fivetran_sdk::DescribeTableResponse response;
            auto status = service.DescribeTable(nullptr, &request, &response);

            INFO(status.error_message());
            REQUIRE(status.ok());
            REQUIRE(!response.not_found());
        }
    }
}