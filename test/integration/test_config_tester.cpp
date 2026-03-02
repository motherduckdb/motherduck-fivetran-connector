#include "../constants.hpp"
#include "common.hpp"
#include "config_tester.hpp"
#include "motherduck_destination_server.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <string>

using namespace test::constants;

TEST_CASE("Test fails when database missing", "[integration][configtest]") {
	::fivetran_sdk::v2::TestRequest request;
	(*request.mutable_configuration())["motherduck_token"] = "12345";

	DestinationSdkImpl service;
	::fivetran_sdk::v2::TestResponse response;
	const auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	CAPTURE(response.failure());
	REQUIRE_THAT(response.failure(), Catch::Matchers::ContainsSubstring("Missing property motherduck_database"));
}

TEST_CASE("Test fails when token is missing", "[integration][configtest]") {
	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_AUTHENTICATE);
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;

	DestinationSdkImpl service;
	::fivetran_sdk::v2::TestResponse response;
	auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	CAPTURE(response.failure());
	REQUIRE_THAT(response.failure(), Catch::Matchers::ContainsSubstring("Missing property motherduck_token"));
}

TEST_CASE("Test endpoint fails when token is bad", "[integration][configtest]") {

	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_AUTHENTICATE);
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	(*request.mutable_configuration())["motherduck_token"] = "12345";

	DestinationSdkImpl service;
	::fivetran_sdk::v2::TestResponse response;
	auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	CAPTURE(response.failure());
	REQUIRE_THAT(response.failure(), Catch::Matchers::ContainsSubstring("not authenticated"));
}

TEST_CASE("Test endpoint authentication test succeeds when everything is in order", "[integration][configtest]") {
	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_AUTHENTICATE);
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;

	DestinationSdkImpl service;
	::fivetran_sdk::v2::TestResponse response;
	const auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	REQUIRE(response.success());
}

TEST_CASE("Test fails when motherduck_database is a share", "[integration][configtest]") {
	auto con = get_test_connection(MD_TOKEN);
	const std::string share_name = "fivetran_test_share";

	// Make sure we are in workspace attach mode
	const auto attach_mode_res = con->Query("SELECT current_setting('motherduck_attach_mode')");
	REQUIRE_NO_FAIL(attach_mode_res);
	REQUIRE(attach_mode_res->RowCount() == 1);
	REQUIRE(attach_mode_res->ColumnCount() == 1);
	const auto attach_mode = attach_mode_res->GetValue(0, 0).ToString();
	REQUIRE(attach_mode == "workspace");

	const auto create_res = con->Query("CREATE OR REPLACE SHARE " + share_name + " FROM " + TEST_DATABASE_NAME);
	REQUIRE_NO_FAIL(create_res);
	REQUIRE(create_res->RowCount() == 1);
	REQUIRE(create_res->ColumnCount() == 1);
	const auto share_url = create_res->GetValue(0, 0).ToString();

	const auto attach_res = con->Query("ATTACH IF NOT EXISTS '" + share_url + "'");
	REQUIRE_NO_FAIL(attach_res);

	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_DATABASE_TYPE);
	(*request.mutable_configuration())["motherduck_database"] = "fivetran_test_share";
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;

	DestinationSdkImpl service;
	::fivetran_sdk::v2::TestResponse response;
	const auto status = service.Test(nullptr, &request, &response);

	con->Query("DETACH IF EXISTS " + share_name);

	REQUIRE_NO_FAIL(status);
	CAPTURE(response.failure());
	REQUIRE_THAT(response.failure(), Catch::Matchers::ContainsSubstring("is a read-only MotherDuck share"));
}

TEST_CASE("Test config tester for max_record_size values", "[integration][configtest]") {
	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_MAX_RECORD_SIZE_VALID);
	auto& request_config = *request.mutable_configuration();
	request_config["motherduck_database"] = TEST_DATABASE_NAME;
	request_config["motherduck_token"] = MD_TOKEN;

	DestinationSdkImpl service;
	::fivetran_sdk::v2::TestResponse response;

	SECTION("Fails when max_record_size is not a number") {
		const std::string config_input = GENERATE("nan", "32MiB");
		request_config["max_record_size"] = config_input;

		request_config["max_record_size"] = config_input;
		auto status = service.Test(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		CAPTURE(response.failure());
		REQUIRE_THAT(response.failure(), Catch::Matchers::ContainsSubstring("contains non-numeric characters"));
	}

	SECTION("Fails when max_record_size is too low") {
		request_config["max_record_size"] = "12";
		auto status = service.Test(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		CAPTURE(response.failure());
		REQUIRE_THAT(response.failure(), Catch::Matchers::ContainsSubstring("lower than the default"));
	}

	SECTION("Fails when max_record_size is too high") {
		request_config["max_record_size"] = "1025";
		auto status = service.Test(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		CAPTURE(response.failure());
		REQUIRE_THAT(response.failure(), Catch::Matchers::ContainsSubstring("is higher than the max"));
	}

	SECTION("Succeeds when max_record_size is a number in allowed range") {
		const std::string config_input = GENERATE("24", "100", "1023");
		request_config["max_record_size"] = config_input;

		auto status = service.Test(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		CAPTURE(response.failure());
		REQUIRE(response.failure().empty());
		REQUIRE(response.success());
	}

	SECTION("Succeeds when max_record_size is empty") {
		request_config["max_record_size"] = "";
		auto status = service.Test(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		CAPTURE(response.failure());
		REQUIRE(response.failure().empty());
		REQUIRE(response.success());
	}
}
