#include "../constants.hpp"
#include "common.hpp"

#include <catch2/catch_test_macros.hpp>
#include <future>
#include <string>
#include <vector>

using namespace test::constants;

TEST_CASE("Parallel WriteBatch requests", "[integration][write-batch][parallel]") {
	DestinationSdkImpl service;

	constexpr unsigned int num_tables = 5;
	std::vector<std::string> table_names;

	for (unsigned int i = 0; i < num_tables; i++) {
		const std::string table_name = "parallel_books_" + std::to_string(i);
		table_names.push_back(table_name);

		::fivetran_sdk::v2::CreateTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		define_test_table(request, table_name);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	// Launch parallel WriteBatch requests that each write to their own table
	std::vector<std::future<grpc::Status>> futures;

	for (unsigned int i = 0; i < num_tables; i++) {
		futures.push_back(std::async(std::launch::async, [&service, &table_names, i]() {
			::fivetran_sdk::v2::WriteBatchRequest request;
			(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
			(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
			define_test_table(request, table_names[i]);
			request.mutable_file_params()->set_null_string("magic-nullvalue");
			request.add_replace_files(TEST_RESOURCES_DIR + "books_upsert.csv");

			::fivetran_sdk::v2::WriteBatchResponse response;
			return service.WriteBatch(nullptr, &request, &response);
		}));
	}

	for (auto& future : futures) {
		auto status = future.get();
		REQUIRE_NO_FAIL(status);
	}

	auto con = get_test_connection(MD_TOKEN);
	for (const auto& table_name : table_names) {
		auto res = con->Query("SELECT id, title FROM " + table_name + " ORDER BY id");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 3);
	}
}

TEST_CASE("Parallel DescribeTable requests", "[integration][describe-table][parallel]") {
	DestinationSdkImpl service;

	constexpr unsigned int num_tables = 10;
	constexpr unsigned int requests_per_table = 5;
	std::vector<std::string> table_names;

	for (unsigned int t = 0; t < num_tables; t++) {
		const std::string table_name = "parallel_describe_" + std::to_string(t);
		table_names.push_back(table_name);

		::fivetran_sdk::v2::CreateTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		define_test_table(request, table_name);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	std::vector<std::future<grpc::Status>> futures;

	for (unsigned int t = 0; t < num_tables; t++) {
		for (unsigned int r = 0; r < requests_per_table; r++) {
			futures.push_back(std::async(std::launch::async, [&service, &table_names, t]() {
				::fivetran_sdk::v2::DescribeTableRequest request;
				(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
				(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
				request.set_table_name(table_names[t]);

				::fivetran_sdk::v2::DescribeTableResponse response;
				return service.DescribeTable(nullptr, &request, &response);
			}));
		}
	}

	for (auto& future : futures) {
		auto status = future.get();
		REQUIRE_NO_FAIL(status);
	}
}

TEST_CASE("Parallel CreateTable requests into the same schema", "[integration][create-table][parallel]") {
	DestinationSdkImpl service;

	constexpr unsigned int num_tables = 10;
	constexpr std::string schema_name = "parallel_schema";

	{
		auto test_con = get_test_connection(MD_TOKEN);
		const auto drop_res = test_con->Query("DROP SCHEMA IF EXISTS " + schema_name + " CASCADE");
		if (drop_res->HasError()) {
			FAIL("Failed to drop schema " + schema_name + " before test: " + drop_res->GetError());
		}
	}

	std::vector<std::future<grpc::Status>> futures;
	for (unsigned int i = 0; i < num_tables; i++) {
		futures.push_back(std::async(std::launch::async, [&service, &schema_name, i]() {
			::fivetran_sdk::v2::CreateTableRequest request;
			(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
			(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
			request.set_schema_name(schema_name);
			define_test_table(request, "parallel_table_" + std::to_string(i));

			::fivetran_sdk::v2::CreateTableResponse response;
			return service.CreateTable(nullptr, &request, &response);
		}));
	}

	for (auto& future : futures) {
		REQUIRE_NO_FAIL(future.get());
	}

	// All tables should exist
	for (unsigned int i = 0; i < num_tables; i++) {
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_schema_name(schema_name);
		request.set_table_name("parallel_table_" + std::to_string(i));

		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE_FALSE(response.not_found());
	}
}

TEST_CASE("Parallel CreateTable requests into different schemas", "[integration][create-table][parallel]") {
	DestinationSdkImpl service;

	constexpr unsigned int num_schemas = 10;
	constexpr std::string schema_base = "parallel_schema_";
	constexpr std::string table_name = "some_table";
	{
		auto test_con = get_test_connection(MD_TOKEN);
		for (unsigned int i = 0; i < num_schemas; i++) {
			const auto schema_name = schema_base + std::to_string(i);
			const auto drop_res = test_con->Query("DROP SCHEMA IF EXISTS " + schema_name + " CASCADE");
			if (drop_res->HasError()) {
				FAIL("Failed to drop schema " + schema_name + " before test: " + drop_res->GetError());
			}
		}
	}

	std::vector<std::future<grpc::Status>> futures;
	for (unsigned int i = 0; i < num_schemas; i++) {
		futures.push_back(std::async(std::launch::async, [&service, &schema_base, &table_name, i]() {
			::fivetran_sdk::v2::CreateTableRequest request;
			(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
			(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
			request.set_schema_name(schema_base + std::to_string(i));
			define_test_table(request, table_name);

			::fivetran_sdk::v2::CreateTableResponse response;
			return service.CreateTable(nullptr, &request, &response);
		}));
	}

	for (auto& future : futures) {
		REQUIRE_NO_FAIL(future.get());
	}

	for (unsigned int i = 0; i < num_schemas; i++) {
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_schema_name(schema_base + std::to_string(i));
		request.set_table_name(table_name);

		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE_FALSE(response.not_found());
	}
}
