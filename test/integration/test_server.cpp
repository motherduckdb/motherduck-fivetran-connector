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


TEST_CASE("DescribeTable", "[integration][now]") {
    DestinationSdkImpl service;

    ::fivetran_sdk::DescribeTableRequest request;
    auto token = std::getenv("motherduck_token");
    REQUIRE(token);
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    ::fivetran_sdk::DescribeTableResponse response;

    auto status = service.DescribeTable(nullptr, &request, &response);

    INFO(status.error_message());
    REQUIRE(status.ok());
}

