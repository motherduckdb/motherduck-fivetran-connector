#include "../constants.hpp"
#include "common.hpp"
#include "config_tester.hpp"
#include "duckdb.hpp"
#include "motherduck_destination_server.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/internal/catch_run_context.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <fstream>
#include <future>
#include <thread>
#include <vector>

using namespace test::constants;

TEST_CASE("ConfigurationForm", "[integration][config]") {
	DestinationSdkImpl service;
	::fivetran_sdk::v2::ConfigurationFormRequest request;
	::fivetran_sdk::v2::ConfigurationFormResponse response;

	auto status = service.ConfigurationForm(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);

	REQUIRE(response.fields_size() == 3);
	REQUIRE(response.fields(0).name() == "motherduck_token");
	REQUIRE(response.fields(1).name() == "motherduck_database");
	REQUIRE(response.fields(2).name() == "max_record_size");

	REQUIRE(response.tests_size() == 4);
}

TEST_CASE("DescribeTable fails when database missing", "[integration][describe-table]") {
	DestinationSdkImpl service;

	::fivetran_sdk::v2::DescribeTableRequest request;
	(*request.mutable_configuration())["motherduck_token"] = "12345";

	::fivetran_sdk::v2::DescribeTableResponse response;

	auto status = service.DescribeTable(nullptr, &request, &response);
	REQUIRE_FAIL(status, "Missing property motherduck_database");
}

TEST_CASE("DescribeTable on nonexistent table", "[integration][describe-table]") {
	DestinationSdkImpl service;

	::fivetran_sdk::v2::DescribeTableRequest request;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	request.set_table_name("nonexistent_table");
	::fivetran_sdk::v2::DescribeTableResponse response;

	auto status = service.DescribeTable(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	REQUIRE(response.not_found());
}

TEST_CASE("CreateTable, DescribeTable for existing table, AlterTable", "[integration]") {
	DestinationSdkImpl service;

	const std::string schema_name = "some_schema" + std::to_string(Catch::rngSeed());
	const std::string table_name = "some_table" + std::to_string(Catch::rngSeed());

	{
		// Create Table
		::fivetran_sdk::v2::CreateTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_schema_name(schema_name);
		request.mutable_table()->set_name(table_name);
		auto col1 = request.mutable_table()->add_columns();
		col1->set_name("id");
		col1->set_type(::fivetran_sdk::v2::DataType::STRING);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Describe the created table
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		{
			// table not found in default "main" schema
			::fivetran_sdk::v2::DescribeTableResponse response;
			auto status = service.DescribeTable(nullptr, &request, &response);
			REQUIRE_NO_FAIL(status);
			REQUIRE(response.not_found());
		}

		{
			// table found in the correct schema
			request.set_schema_name(schema_name);
			::fivetran_sdk::v2::DescribeTableResponse response;
			auto status = service.DescribeTable(nullptr, &request, &response);
			REQUIRE_NO_FAIL(status);
			REQUIRE(!response.not_found());

			REQUIRE(response.table().name() == table_name);
			REQUIRE(response.table().columns_size() == 1);
			REQUIRE(response.table().columns(0).name() == "id");
			REQUIRE(response.table().columns(0).type() == ::fivetran_sdk::v2::DataType::STRING);
			REQUIRE_FALSE(response.table().columns(0).has_params());
			REQUIRE_FALSE(response.table().columns(0).params().has_decimal());
		}
	}

	{
		// Alter Table
		::fivetran_sdk::v2::AlterTableRequest request;

		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_schema_name(schema_name);
		request.mutable_table()->set_name(table_name);
		::fivetran_sdk::v2::Column col1;
		col1.set_name("id");
		col1.set_type(::fivetran_sdk::v2::DataType::INT);
		request.mutable_table()->add_columns()->CopyFrom(col1);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Describe the altered table
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		// table found in the correct schema
		request.set_schema_name(schema_name);
		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE(!response.not_found());

		REQUIRE(response.table().name() == table_name);
		REQUIRE(response.table().columns_size() == 1);
		REQUIRE(response.table().columns(0).name() == "id");
		REQUIRE(response.table().columns(0).type() == ::fivetran_sdk::v2::DataType::INT);
	}
}

TEST_CASE("Test fails when database missing", "[integration][configtest]") {
	DestinationSdkImpl service;

	::fivetran_sdk::v2::TestRequest request;
	(*request.mutable_configuration())["motherduck_token"] = "12345";

	::fivetran_sdk::v2::TestResponse response;

	const auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	REQUIRE_THAT(response.failure(), Catch::Matchers::ContainsSubstring("Missing property motherduck_database"));
}

TEST_CASE("Test fails when token is missing", "[integration][configtest]") {
	DestinationSdkImpl service;

	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_AUTHENTICATE);
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;

	::fivetran_sdk::v2::TestResponse response;

	auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	const std::string expected_message = "Test <test_authentication> failed: Missing property motherduck_token";
	REQUIRE(response.failure() == expected_message);
}

TEST_CASE("Test endpoint fails when token is bad", "[integration][configtest]") {
	DestinationSdkImpl service;

	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_AUTHENTICATE);
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	(*request.mutable_configuration())["motherduck_token"] = "12345";

	::fivetran_sdk::v2::TestResponse response;

	auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	REQUIRE_THAT(response.failure(), Catch::Matchers::ContainsSubstring("not authenticated"));
}

TEST_CASE("Test endpoint authentication test succeeds when everything is in order", "[integration][configtest]") {
	DestinationSdkImpl service;

	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_AUTHENTICATE);
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;

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

	DestinationSdkImpl service;
	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_DATABASE_TYPE);
	(*request.mutable_configuration())["motherduck_database"] = "fivetran_test_share";
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;

	::fivetran_sdk::v2::TestResponse response;

	const auto status = service.Test(nullptr, &request, &response);

	con->Query("DETACH IF EXISTS " + share_name);

	REQUIRE_NO_FAIL(status);
	REQUIRE_THAT(response.failure(), Catch::Matchers::ContainsSubstring("is a read-only MotherDuck share"));
}

TEST_CASE("Test fails when max_record_size is not a number", "[integration][configtest]") {
	DestinationSdkImpl service;

	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_MAX_RECORD_SIZE_VALID);
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
	(*request.mutable_configuration())["max_record_size"] = "nan";

	::fivetran_sdk::v2::TestResponse response;

	auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	const std::string expected_message = "Test <test_max_record_size_valid> failed: Value \"nan\" could not be "
	                                     "converted into an integer for \"Max Record Size\". Make sure to set the "
	                                     "\"Max Record Size\" to a valid positive integer.";
	REQUIRE(response.failure() == expected_message);
}

TEST_CASE("Test succeeds when max_record_size is almost a number", "[integration][configtest]") {
	DestinationSdkImpl service;

	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_MAX_RECORD_SIZE_VALID);
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
	(*request.mutable_configuration())["max_record_size"] = "32MiB";

	::fivetran_sdk::v2::TestResponse response;

	auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	REQUIRE(response.failure() == "");
}

TEST_CASE("Test fails when max_record_size is too low", "[integration][configtest]") {
	DestinationSdkImpl service;

	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_MAX_RECORD_SIZE_VALID);
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
	(*request.mutable_configuration())["max_record_size"] = "12";

	::fivetran_sdk::v2::TestResponse response;

	auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	REQUIRE(response.failure() == "Test <test_max_record_size_valid> failed: Value \"12\" for \"Max Record Size\" "
	                              "is lower than the default of 24 MiB. It should be between 24 and 1024");
}

TEST_CASE("Test fails when max_record_size is too high", "[integration][configtest]") {
	DestinationSdkImpl service;

	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_MAX_RECORD_SIZE_VALID);
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
	(*request.mutable_configuration())["max_record_size"] = "1025";

	::fivetran_sdk::v2::TestResponse response;

	auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	REQUIRE(response.failure() == "Test <test_max_record_size_valid> failed: Value \"1025\" for \"Max Record Size\" "
	                              "is higher than the max of 1024 MiB. It should be between 24 and 1024");
}

TEST_CASE("Test succeeds when max_record_size is a number", "[integration][configtest]") {
	DestinationSdkImpl service;

	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_MAX_RECORD_SIZE_VALID);
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
	(*request.mutable_configuration())["max_record_size"] = "1023";

	::fivetran_sdk::v2::TestResponse response;

	auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	REQUIRE(response.failure().empty());
	REQUIRE(response.success());
}

TEST_CASE("Test succeeds when max_record_size is empty", "[integration][configtest]") {
	DestinationSdkImpl service;

	::fivetran_sdk::v2::TestRequest request;
	request.set_name(config_tester::TEST_MAX_RECORD_SIZE_VALID);
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
	(*request.mutable_configuration())["max_record_size"] = "";

	::fivetran_sdk::v2::TestResponse response;

	auto status = service.Test(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	REQUIRE(response.failure().empty());
	REQUIRE(response.success());
}

TEST_CASE("WriteBatch", "[integration][write-batch]") {
	DestinationSdkImpl service;

	// Schema will be main
	const std::string table_name = "books" + std::to_string(Catch::rngSeed());

	{
		// Create Table
		::fivetran_sdk::v2::CreateTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		define_test_table(request, table_name);

		::fivetran_sdk::v2::CreateTableResponse response;
		const auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	auto con = get_test_connection(MD_TOKEN);
	{
		// insert rows from encrypted / compressed file
		::fivetran_sdk::v2::WriteBatchRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.mutable_file_params()->set_encryption(::fivetran_sdk::v2::Encryption::AES);
		request.mutable_file_params()->set_compression(::fivetran_sdk::v2::Compression::ZSTD);
		define_test_table(request, table_name);
		const std::string filename = "books_batch_1_insert.csv.zst.aes";
		const std::string filepath = TEST_RESOURCES_DIR + filename;

		std::ifstream keyfile(filepath + ".key", std::ios::binary);
		char key[33];
		keyfile.read(key, 32);
		key[32] = 0;
		(*request.mutable_keys())[filepath] = key;

		request.add_replace_files(filepath);

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// check inserted rows
		auto res = con->Query("SELECT id, title, magic_number FROM " + table_name + " ORDER BY id");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 2);
		REQUIRE(res->GetValue(0, 0) == 1);
		REQUIRE(res->GetValue(1, 0) == "The Hitchhiker's Guide to the Galaxy");
		REQUIRE(res->GetValue(2, 0) == 42);

		REQUIRE(res->GetValue(0, 1) == 2);
		REQUIRE(res->GetValue(1, 1) == "The Lord of the Rings");
		REQUIRE(res->GetValue(2, 1) == 1);
	}

	{
		// upsert
		::fivetran_sdk::v2::WriteBatchRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		define_test_table(request, table_name);
		request.mutable_file_params()->set_null_string("magic-nullvalue");
		const std::string filename = "books_upsert.csv";
		const std::string filepath = TEST_RESOURCES_DIR + filename;

		request.add_replace_files(filepath);

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// check after upsert
		auto res = con->Query("SELECT id, title, magic_number FROM " + table_name + " ORDER BY id");
		REQUIRE_NO_FAIL(res);

		REQUIRE(res->RowCount() == 4);
		REQUIRE(res->GetValue(0, 0) == 1);
		REQUIRE(res->GetValue(1, 0) == "The Hitchhiker's Guide to the Galaxy");
		REQUIRE(res->GetValue(2, 0) == 42);

		REQUIRE(res->GetValue(0, 1) == 2);
		REQUIRE(res->GetValue(1, 1) == "The Two Towers"); // updated value
		REQUIRE(res->GetValue(2, 1) == 1);

		// new row
		REQUIRE(res->GetValue(0, 2) == 3);
		REQUIRE(res->GetValue(1, 2) == "The Hobbit");
		REQUIRE(res->GetValue(2, 2) == 14);

		// new row with null value
		REQUIRE(res->GetValue(0, 3) == 99);
		REQUIRE(res->GetValue(1, 3).IsNull() == false); // a string with text "null" should not be null
		REQUIRE(res->GetValue(1, 3) == "null");
		REQUIRE(res->GetValue(2, 3).IsNull() == true);
	}

	{
		// delete
		::fivetran_sdk::v2::WriteBatchRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		define_test_table(request, table_name);
		const std::string filename = "books_delete.csv";
		const std::string filepath = TEST_RESOURCES_DIR + filename;

		request.add_delete_files(filepath);

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// check after delete
		auto res = con->Query("SELECT id, title, magic_number FROM " + table_name + " ORDER BY id");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 3);

		// row 1 got deleted
		REQUIRE(res->GetValue(0, 0) == 2);
		REQUIRE(res->GetValue(1, 0) == "The Two Towers");
		REQUIRE(res->GetValue(2, 0) == 1);

		REQUIRE(res->GetValue(0, 1) == 3);
		REQUIRE(res->GetValue(1, 1) == "The Hobbit");
		REQUIRE(res->GetValue(2, 1) == 14);

		REQUIRE(res->GetValue(0, 2) == 99);
		REQUIRE(res->GetValue(1, 2) == "null");
		REQUIRE(res->GetValue(2, 2).IsNull() == true);
	}

	{
		// update
		::fivetran_sdk::v2::WriteBatchRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.mutable_file_params()->set_unmodified_string("unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
		request.mutable_file_params()->set_null_string("magic-nullvalue");
		define_test_table(request, table_name);
		const std::string filename = "books_update.csv";
		const std::string filepath = TEST_RESOURCES_DIR + filename;

		request.add_update_files(filepath);

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// check after update
		auto res = con->Query("SELECT id, title, magic_number FROM " + table_name + " ORDER BY id");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 3);

		REQUIRE(res->GetValue(0, 0) == 2);
		REQUIRE(res->GetValue(1, 0) == "The empire strikes back");
		REQUIRE(res->GetValue(2, 0) == 1);

		REQUIRE(res->GetValue(0, 1) == 3);
		REQUIRE(res->GetValue(1, 1) == "The Hobbit");
		REQUIRE(res->GetValue(2, 1) == 15); // updated value

		REQUIRE(res->GetValue(0, 2) == 99);
		REQUIRE(res->GetValue(1, 2).IsNull() == true);
		REQUIRE(res->GetValue(2, 2).IsNull() == false);
		REQUIRE(res->GetValue(2, 2) == 99);
	}

	{
		// truncate data before Jan 9 2024
		::fivetran_sdk::v2::TruncateRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);
		request.set_synced_column("_fivetran_synced");
		request.mutable_soft()->set_deleted_column("_fivetran_deleted");

		const auto cutoff_datetime = 1707436800; // 2024-02-09 0:0:0 GMT, trust me
		request.mutable_utc_delete_before()->set_seconds(cutoff_datetime);
		request.mutable_utc_delete_before()->set_nanos(0);
		::fivetran_sdk::v2::TruncateResponse response;
		auto status = service.Truncate(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// check truncated table
		auto res = con->Query("SELECT title, id, magic_number FROM " + table_name +
		                      " WHERE _fivetran_deleted = false ORDER BY id");
		REQUIRE_NO_FAIL(res);
		// the 1st row from books_update.csv that had 2024-02-08T23:59:59.999999999Z
		// timestamp got deleted
		REQUIRE(res->RowCount() == 1);
		REQUIRE(res->GetValue(0, 0) == "The empire strikes back");
		REQUIRE(res->GetValue(1, 0) == 2);
		REQUIRE(res->GetValue(2, 0) == 1);
	}

	{
		// check the rows did not get physically deleted
		auto res = con->Query("SELECT title, id, magic_number FROM " + table_name + " ORDER BY id");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 3);
	}

	{
		// truncate table does nothing if there is no utc_delete_before field set
		::fivetran_sdk::v2::TruncateRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);
		request.set_synced_column("_fivetran_synced");
		request.mutable_soft()->set_deleted_column("_fivetran_deleted");

		::fivetran_sdk::v2::TruncateResponse response;
		auto status = service.Truncate(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// check truncated table is the same as before
		auto res = con->Query("SELECT title FROM " + table_name + " WHERE _fivetran_deleted = false ORDER BY id");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 1);
		REQUIRE(res->GetValue(0, 0) == "The empire strikes back");
	}

	{
		// check again that the rows did not get physically deleted
		auto res = con->Query("SELECT title, id, magic_number FROM " + table_name + " ORDER BY id");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 3);
	}

	{
		// hard truncate all data (deleted_column not set in request)
		::fivetran_sdk::v2::TruncateRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);
		request.set_synced_column("_fivetran_synced");

		const auto cutoff_datetime = 1893456000; // delete everything before 2030-01-01
		request.mutable_utc_delete_before()->set_seconds(cutoff_datetime);
		request.mutable_utc_delete_before()->set_nanos(0);
		::fivetran_sdk::v2::TruncateResponse response;
		auto status = service.Truncate(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// check the rows got physically deleted
		auto res = con->Query("SELECT title, id, magic_number FROM " + table_name + " ORDER BY id");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 0);
	}
}

TEST_CASE("Table with multiple primary keys", "[integration][write-batch]") {
	DestinationSdkImpl service;

	const std::string table_name = "multikey_table" + std::to_string(Catch::rngSeed());

	{
		// Create Table
		::fivetran_sdk::v2::CreateTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		define_test_multikey_table(request, table_name);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Describe the created table
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		{
			::fivetran_sdk::v2::DescribeTableResponse response;
			auto status = service.DescribeTable(nullptr, &request, &response);
			REQUIRE_NO_FAIL(status);
			REQUIRE(response.table().columns().size() == 5);

			REQUIRE(response.table().columns(0).name() == "id1");
			REQUIRE(response.table().columns(1).name() == "id2");
			REQUIRE(response.table().columns(2).name() == "text");
			REQUIRE(response.table().columns(3).name() == "_fivetran_deleted");
			REQUIRE(response.table().columns(4).name() == "_fivetran_synced");
		}
	}

	// test connection needs to be created after table creation to avoid stale
	// catalog
	auto con = get_test_connection(MD_TOKEN);
	{
		// insert rows
		::fivetran_sdk::v2::WriteBatchRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		define_test_multikey_table(request, table_name);
		const std::string filename = "multikey_table_upsert.csv";
		const std::string filepath = TEST_RESOURCES_DIR + filename;

		request.add_replace_files(filepath);

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// check inserted rows
		auto res = con->Query("SELECT id1, id2, text FROM " + table_name + " ORDER BY id1, id2");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 3);
		REQUIRE(res->GetValue(0, 0) == 1);
		REQUIRE(res->GetValue(1, 0) == 100);
		REQUIRE(res->GetValue(2, 0) == "first row");

		REQUIRE(res->GetValue(0, 1) == 2);
		REQUIRE(res->GetValue(1, 1) == 200);
		REQUIRE(res->GetValue(2, 1) == "second row");

		REQUIRE(res->GetValue(0, 2) == 3);
		REQUIRE(res->GetValue(1, 2) == 300);
		REQUIRE(res->GetValue(2, 2) == "third row");
	}

	{
		// update
		::fivetran_sdk::v2::WriteBatchRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.mutable_file_params()->set_unmodified_string("magic-unmodified-value");
		request.mutable_file_params()->set_null_string("magic-nullvalue");
		define_test_multikey_table(request, table_name);
		const std::string filename = "multikey_table_update.csv";
		const std::string filepath = TEST_RESOURCES_DIR + filename;

		request.add_update_files(filepath);

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// check after update, including a soft delete
		auto res = con->Query("SELECT id1, id2, text, _fivetran_deleted FROM " + table_name + " ORDER BY id1, id2");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 3);
		REQUIRE(res->GetValue(0, 0) == 1);
		REQUIRE(res->GetValue(1, 0) == 100);
		REQUIRE(res->GetValue(2, 0) == "first row");
		REQUIRE(res->GetValue(3, 0) == false);

		REQUIRE(res->GetValue(0, 1) == 2);
		REQUIRE(res->GetValue(1, 1) == 200);
		REQUIRE(res->GetValue(2, 1) == "second row updated");
		REQUIRE(res->GetValue(3, 1) == false);

		REQUIRE(res->GetValue(0, 2) == 3);
		REQUIRE(res->GetValue(1, 2) == 300);
		REQUIRE(res->GetValue(2, 2) == "third row soft deleted - but also this value updated");
		REQUIRE(res->GetValue(3, 2) == true);
	}

	{
		// delete
		::fivetran_sdk::v2::WriteBatchRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		define_test_multikey_table(request, table_name);
		const std::string filename = "multikey_table_delete.csv";
		const std::string filepath = TEST_RESOURCES_DIR + filename;

		request.add_delete_files(filepath);

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// check after hard delete
		auto res = con->Query("SELECT id1, id2, text, _fivetran_deleted FROM " + table_name + " ORDER BY id1, id2");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 1);
		REQUIRE(res->GetValue(0, 0) == 2);
		REQUIRE(res->GetValue(1, 0) == 200);
		REQUIRE(res->GetValue(2, 0) == "second row updated");
		REQUIRE(res->GetValue(3, 0) == false);
	}
}

TEST_CASE("CreateTable with JSON column", "[integration]") {
	DestinationSdkImpl service;

	const std::string table_name = "json_table" + std::to_string(Catch::rngSeed());

	{
		// Create Table
		::fivetran_sdk::v2::CreateTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.mutable_table()->set_name(table_name);
		auto col1 = request.mutable_table()->add_columns();
		col1->set_name("data");
		col1->set_type(::fivetran_sdk::v2::DataType::JSON);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Describe the created table
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		{
			::fivetran_sdk::v2::DescribeTableResponse response;
			auto status = service.DescribeTable(nullptr, &request, &response);
			REQUIRE_NO_FAIL(status);
			REQUIRE(response.table().columns().size() == 1);
			REQUIRE(response.table().columns(0).type() == ::fivetran_sdk::v2::DataType::STRING);
		}
	}
}

TEST_CASE("Parallel WriteBatch requests", "[integration][write-batch]") {
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

TEST_CASE("Parallel DescribeTable requests", "[integration][describe-table]") {
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

TEST_CASE("Truncate nonexistent table should succeed", "[integration]") {
	DestinationSdkImpl service;

	const std::string bad_table_name = "nonexistent";

	::fivetran_sdk::v2::TruncateRequest request;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	request.set_schema_name("some_schema");
	request.set_table_name(bad_table_name);
	request.set_synced_column("_fivetran_synced");
	request.mutable_soft()->set_deleted_column("_fivetran_deleted");

	::fivetran_sdk::v2::TruncateResponse response;

	std::stringstream buffer;
	std::streambuf* real_cout = std::cout.rdbuf(buffer.rdbuf());
	auto status = service.Truncate(nullptr, &request, &response);
	std::cout.rdbuf(real_cout);

	REQUIRE_NO_FAIL(status);
	REQUIRE_THAT(buffer.str(), Catch::Matchers::ContainsSubstring("Table <nonexistent> not found in schema "
	                                                              "<some_schema>; not truncated"));
}

TEST_CASE("Truncate fails if synced_column is missing") {

	DestinationSdkImpl service;

	const std::string bad_table_name = "nonexistent";

	::fivetran_sdk::v2::TruncateRequest request;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	request.set_table_name("some_table");

	::fivetran_sdk::v2::TruncateResponse response;
	auto status = service.Truncate(nullptr, &request, &response);

	REQUIRE_FAIL(status, "Synced column is required");
}

TEST_CASE("reading inaccessible or nonexistent files fails") {
	DestinationSdkImpl service;

	const std::string bad_file_name = TEST_RESOURCES_DIR + "nonexistent.csv";
	::fivetran_sdk::v2::WriteBatchRequest request;

	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	request.mutable_file_params()->set_encryption(::fivetran_sdk::v2::Encryption::AES);
	request.mutable_file_params()->set_compression(::fivetran_sdk::v2::Compression::ZSTD);
	define_test_table(request, "unused_table");

	request.add_replace_files(bad_file_name);
	(*request.mutable_keys())[bad_file_name] = "whatever";

	::fivetran_sdk::v2::WriteBatchResponse response;
	auto status = service.WriteBatch(nullptr, &request, &response);
	REQUIRE_FALSE(status.ok());
	REQUIRE_THAT(status.error_message(), Catch::Matchers::ContainsSubstring("No such file or directory"));
}

TEST_CASE("WriteBatch fails with invalid max_record_size", "[integration][write-batch]") {
	DestinationSdkImpl service;

	const std::string table_name = "people_history";

	::fivetran_sdk::v2::WriteBatchRequest request;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	(*request.mutable_configuration())["max_record_size"] = "not_a_number";
	request.set_schema_name("test");
	define_test_table(request, table_name);

	request.add_replace_files(TEST_RESOURCES_DIR + "books_upsert.csv");

	::fivetran_sdk::v2::WriteBatchResponse response;
	auto status = service.WriteBatch(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	REQUIRE(response.mutable_task()->message() ==
	        "WriteBatch endpoint failed for schema <test>, table "
	        "<people_history>: Value \"not_a_number\" could not be converted into an integer for \"Max "
	        "Record Size\". Make sure to set the \"Max Record Size\" to a valid positive integer.");
}

TEST_CASE("Test all types with create and describe table") {

	DestinationSdkImpl service;

	const std::string table_name = "all_types_table" + std::to_string(Catch::rngSeed());

	{
		// Create Table
		::fivetran_sdk::v2::CreateTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.mutable_table()->set_name(table_name);
		auto col1 = request.mutable_table()->add_columns();
		col1->set_name("col_string");
		col1->set_type(::fivetran_sdk::v2::DataType::STRING);
		col1->set_primary_key(true);
		auto col2 = request.mutable_table()->add_columns();
		col2->set_name("col_int");
		col2->set_type(::fivetran_sdk::v2::DataType::INT);
		col2->set_primary_key(true);
		auto col3 = request.mutable_table()->add_columns();
		col3->set_name("col_decimal");
		col3->set_type(::fivetran_sdk::v2::DataType::DECIMAL);
		col3->mutable_params()->mutable_decimal()->set_precision(20);
		col3->mutable_params()->mutable_decimal()->set_scale(11);

		auto col4 = request.mutable_table()->add_columns();
		col4->set_name("col_utc_datetime");
		col4->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);
		auto col5 = request.mutable_table()->add_columns();
		col5->set_name("col_naive_datetime");
		col5->set_type(::fivetran_sdk::v2::DataType::NAIVE_DATETIME);
		auto col6 = request.mutable_table()->add_columns();
		col6->set_name("col_naive_date");
		col6->set_type(::fivetran_sdk::v2::DataType::NAIVE_DATE);

		auto col7 = request.mutable_table()->add_columns();
		col7->set_name("col_boolean");
		col7->set_type(::fivetran_sdk::v2::DataType::BOOLEAN);
		auto col8 = request.mutable_table()->add_columns();
		col8->set_name("col_short");
		col8->set_type(::fivetran_sdk::v2::DataType::SHORT);
		auto col9 = request.mutable_table()->add_columns();
		col9->set_name("col_long");
		col9->set_type(::fivetran_sdk::v2::DataType::LONG);
		auto col10 = request.mutable_table()->add_columns();
		col10->set_name("col_float");
		col10->set_type(::fivetran_sdk::v2::DataType::FLOAT);
		auto col11 = request.mutable_table()->add_columns();
		col11->set_name("col_double");
		col11->set_type(::fivetran_sdk::v2::DataType::DOUBLE);
		auto col12 = request.mutable_table()->add_columns();
		col12->set_name("col_binary");
		col12->set_type(::fivetran_sdk::v2::DataType::BINARY);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Describe table
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE(!response.not_found());

		REQUIRE(response.table().name() == table_name);
		REQUIRE(response.table().columns_size() == 12);

		REQUIRE(response.table().columns(0).name() == "col_string");
		REQUIRE(response.table().columns(0).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE(response.table().columns(0).primary_key());
		REQUIRE(response.table().columns(1).name() == "col_int");
		REQUIRE(response.table().columns(1).type() == ::fivetran_sdk::v2::DataType::INT);
		REQUIRE(response.table().columns(1).primary_key());

		REQUIRE(response.table().columns(2).name() == "col_decimal");
		REQUIRE(response.table().columns(2).type() == ::fivetran_sdk::v2::DataType::DECIMAL);
		REQUIRE(response.table().columns(2).has_params());
		REQUIRE(response.table().columns(2).params().has_decimal());

		REQUIRE(response.table().columns(2).params().decimal().scale() == 11);
		REQUIRE(response.table().columns(2).params().decimal().precision() == 20);
		REQUIRE_FALSE(response.table().columns(2).primary_key());

		REQUIRE(response.table().columns(3).name() == "col_utc_datetime");
		REQUIRE(response.table().columns(3).type() == ::fivetran_sdk::v2::DataType::UTC_DATETIME);
		REQUIRE_FALSE(response.table().columns(3).primary_key());
		REQUIRE(response.table().columns(4).name() == "col_naive_datetime");
		REQUIRE(response.table().columns(4).type() == ::fivetran_sdk::v2::DataType::NAIVE_DATETIME);
		REQUIRE_FALSE(response.table().columns(4).primary_key());
		REQUIRE(response.table().columns(5).name() == "col_naive_date");
		REQUIRE(response.table().columns(5).type() == ::fivetran_sdk::v2::DataType::NAIVE_DATE);
		REQUIRE_FALSE(response.table().columns(5).primary_key());

		REQUIRE(response.table().columns(6).name() == "col_boolean");
		REQUIRE(response.table().columns(6).type() == ::fivetran_sdk::v2::DataType::BOOLEAN);
		REQUIRE_FALSE(response.table().columns(6).primary_key());
		REQUIRE(response.table().columns(7).name() == "col_short");
		REQUIRE(response.table().columns(7).type() == ::fivetran_sdk::v2::DataType::SHORT);
		REQUIRE_FALSE(response.table().columns(7).primary_key());
		REQUIRE(response.table().columns(8).name() == "col_long");
		REQUIRE(response.table().columns(8).type() == ::fivetran_sdk::v2::DataType::LONG);
		REQUIRE_FALSE(response.table().columns(8).primary_key());
		REQUIRE(response.table().columns(9).name() == "col_float");
		REQUIRE(response.table().columns(9).type() == ::fivetran_sdk::v2::DataType::FLOAT);
		REQUIRE_FALSE(response.table().columns(9).primary_key());
		REQUIRE(response.table().columns(10).name() == "col_double");
		REQUIRE(response.table().columns(10).type() == ::fivetran_sdk::v2::DataType::DOUBLE);
		REQUIRE_FALSE(response.table().columns(10).primary_key());
		REQUIRE(response.table().columns(11).name() == "col_binary");
		REQUIRE(response.table().columns(11).type() == ::fivetran_sdk::v2::DataType::BINARY);
		REQUIRE_FALSE(response.table().columns(11).primary_key());
	}
}

TEST_CASE("Test that error is thrown for invalid DECIMAL width and scale") {
	DestinationSdkImpl service;

	// Try use DECIMAL column with precision/width > 38
	::fivetran_sdk::v2::CreateTableRequest request;
	(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
	(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
	request.mutable_table()->set_name("my_decimal_table");

	SECTION("Test precision/width > 38") {
		auto decimal_col = request.mutable_table()->add_columns();
		decimal_col->set_name("col_decimal");
		decimal_col->set_type(::fivetran_sdk::v2::DataType::DECIMAL);
		decimal_col->mutable_params()->mutable_decimal()->set_precision(39);
		decimal_col->mutable_params()->mutable_decimal()->set_scale(5);
		decimal_col->set_primary_key(true);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_FALSE(status.ok());
		REQUIRE_THAT(status.error_message(), Catch::Matchers::ContainsSubstring("maximum supported width of 38"));
	}

	SECTION("Test scale > precision/width") {
		auto decimal_col = request.mutable_table()->add_columns();
		decimal_col->set_name("col_decimal");
		decimal_col->set_type(::fivetran_sdk::v2::DataType::DECIMAL);
		decimal_col->mutable_params()->mutable_decimal()->set_precision(10);
		decimal_col->mutable_params()->mutable_decimal()->set_scale(15);
		decimal_col->set_primary_key(true);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_FALSE(status.ok());
		REQUIRE_THAT(status.error_message(), Catch::Matchers::ContainsSubstring("cannot be greater than precision"));
	}
}

TEST_CASE("AlterTable with constraints", "[integration]") {
	DestinationSdkImpl service;

	const std::string table_name = "some_table" + std::to_string(Catch::rngSeed());

	auto con = get_test_connection(MD_TOKEN);

	{
		// Create Table
		::fivetran_sdk::v2::CreateTableRequest request;
		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, false);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Alter Table to add a new primary key to an empty table
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, false);
		add_col(request, "id_new", ::fivetran_sdk::v2::DataType::INT, true);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Describe the altered table
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE(!response.not_found());

		REQUIRE(response.table().name() == table_name);
		REQUIRE(response.table().columns_size() == 3);
		REQUIRE(response.table().columns(0).name() == "id");
		REQUIRE(response.table().columns(0).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE(response.table().columns(0).primary_key());

		REQUIRE(response.table().columns(1).name() == "name");
		REQUIRE(response.table().columns(1).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE_FALSE(response.table().columns(1).primary_key());

		REQUIRE(response.table().columns(2).name() == "id_new");
		REQUIRE(response.table().columns(2).type() == ::fivetran_sdk::v2::DataType::INT);
		REQUIRE(response.table().columns(2).primary_key());
	}

	{
		// Insert some test data to validate after primary key modification
		auto res = con->Query("INSERT INTO " + table_name + "(id, name, id_new) VALUES (1, 'one', 101)");
		REQUIRE_NO_FAIL(res);
		auto res2 = con->Query("INSERT INTO " + table_name + "(id, name, id_new) VALUES (2, 'two', 102)");
		REQUIRE_NO_FAIL(res2);
	}

	{
		// Alter Table to make an existing (unique valued) column into a primary key
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
		// turn existing column into a primary key
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, true);
		add_col(request, "id_new", ::fivetran_sdk::v2::DataType::INT, true);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Describe the altered table
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE(!response.not_found());

		REQUIRE(response.table().name() == table_name);
		REQUIRE(response.table().columns_size() == 3);
		REQUIRE(response.table().columns(0).name() == "id");
		REQUIRE(response.table().columns(0).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE(response.table().columns(0).primary_key());

		REQUIRE(response.table().columns(1).name() == "name");
		REQUIRE(response.table().columns(1).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE(response.table().columns(1).primary_key());

		REQUIRE(response.table().columns(2).name() == "id_new");
		REQUIRE(response.table().columns(2).type() == ::fivetran_sdk::v2::DataType::INT);
		REQUIRE(response.table().columns(2).primary_key());
	}

	{
		// Make sure the data is still correct after recreating the table
		auto res = con->Query("SELECT id, name, id_new FROM " + table_name);
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 2);
		REQUIRE(res->GetValue(0, 0) == "1");
		REQUIRE(res->GetValue(1, 0) == "one");
		REQUIRE(res->GetValue(2, 0) == 101);
		REQUIRE(res->GetValue(0, 1) == "2");
		REQUIRE(res->GetValue(1, 1) == "two");
		REQUIRE(res->GetValue(2, 1) == 102);
	}

	{
		// Alter Table to drop a primary key column -- should be a no-op as dropping
		// columns is not allowed
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, true);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Describe the altered table
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE(!response.not_found());

		REQUIRE(response.table().name() == table_name);
		REQUIRE(response.table().columns_size() == 3);
		REQUIRE(response.table().columns(0).name() == "id");
		REQUIRE(response.table().columns(0).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE(response.table().columns(0).primary_key());

		REQUIRE(response.table().columns(1).name() == "name");
		REQUIRE(response.table().columns(1).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE(response.table().columns(1).primary_key());

		REQUIRE(response.table().columns(2).name() == "id_new");
		REQUIRE(response.table().columns(2).type() == ::fivetran_sdk::v2::DataType::INT);
		REQUIRE(response.table().columns(2).primary_key());
	}

	{
		// Make sure the data is still correct after recreating the table
		auto res = con->Query("SELECT id, name FROM " + table_name);
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 2);
		REQUIRE(res->GetValue(0, 0) == "1");
		REQUIRE(res->GetValue(1, 0) == "one");
		REQUIRE(res->GetValue(0, 1) == "2");
		REQUIRE(res->GetValue(1, 1) == "two");
	}

	{
		// Alter Table to add new primary key columns with correct defaults
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, true);
		// id_new is missing but will not be dropped
		add_col(request, "id_int", ::fivetran_sdk::v2::DataType::INT, true);
		add_col(request, "id_varchar", ::fivetran_sdk::v2::DataType::STRING, true);
		add_col(request, "id_date", ::fivetran_sdk::v2::DataType::NAIVE_DATE, true);
		add_col(request, "id_float", ::fivetran_sdk::v2::DataType::FLOAT, true);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{

		// Make sure the defaults are set correctly
		auto res = con->Query("SELECT * FROM " + table_name);
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 2);
		REQUIRE(res->GetValue(0, 0) == "1");
		REQUIRE(res->GetValue(1, 0) == "one");
		REQUIRE(res->GetValue(2, 0) == 0);            // id_new that did not get deleted
		REQUIRE(res->GetValue(3, 0) == 0);            // id_int that got added
		REQUIRE(res->GetValue(4, 0) == "");           // id_varchar that got added
		REQUIRE(res->GetValue(5, 0) == "1970-01-01"); // id_date that got added
		REQUIRE(res->GetValue(6, 0) == 0.0);          // id_float that got added
	}

	{
		// Alter Table make a column no longer be a primary key AND change its type
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, true);

		add_col(request, "id_int", ::fivetran_sdk::v2::DataType::LONG, false);
		add_col(request, "id_varchar", ::fivetran_sdk::v2::DataType::STRING, true);
		add_col(request, "id_date", ::fivetran_sdk::v2::DataType::NAIVE_DATE, true);
		add_col(request, "id_float", ::fivetran_sdk::v2::DataType::FLOAT, true);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Describe the altered table
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE(!response.not_found());

		REQUIRE(response.table().name() == table_name);
		REQUIRE(response.table().columns_size() == 7);
		REQUIRE(response.table().columns(0).name() == "id");
		REQUIRE(response.table().columns(0).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE(response.table().columns(0).primary_key());

		REQUIRE(response.table().columns(1).name() == "name");
		REQUIRE(response.table().columns(1).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE(response.table().columns(1).primary_key());

		REQUIRE(response.table().columns(2).name() == "id_new");
		REQUIRE(response.table().columns(2).type() == ::fivetran_sdk::v2::DataType::INT);
		REQUIRE(response.table().columns(2).primary_key());

		REQUIRE(response.table().columns(3).name() == "id_int");
		REQUIRE(response.table().columns(3).type() == ::fivetran_sdk::v2::DataType::LONG); // this type got updated
		REQUIRE_FALSE(response.table().columns(3).primary_key());

		REQUIRE(response.table().columns(4).name() == "id_varchar");
		REQUIRE(response.table().columns(4).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE(response.table().columns(4).primary_key());

		REQUIRE(response.table().columns(5).name() == "id_date");
		REQUIRE(response.table().columns(5).type() == ::fivetran_sdk::v2::DataType::NAIVE_DATE);
		REQUIRE(response.table().columns(5).primary_key());

		REQUIRE(response.table().columns(6).name() == "id_float");
		REQUIRE(response.table().columns(6).type() == ::fivetran_sdk::v2::DataType::FLOAT);
		REQUIRE(response.table().columns(6).primary_key());
	}

	{
		// Make sure the defaults are set correctly
		auto res = con->Query("SELECT * FROM " + table_name);
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 2);
		REQUIRE(res->GetValue(0, 0) == "1");
		REQUIRE(res->GetValue(1, 0) == "one");
		REQUIRE(res->GetValue(2, 0) == 0);
		REQUIRE(res->GetValue(3, 0) == 0);
		REQUIRE(res->GetValue(4, 0) == "");
		REQUIRE(res->GetValue(5, 0) == "1970-01-01");
		REQUIRE(res->GetValue(6, 0) == 0.0);
	}
}

TEST_CASE("Invalid truncate with nonexisting delete column", "[integration][current]") {
	DestinationSdkImpl service;

	const std::string table_name = "empty_table" + std::to_string(Catch::rngSeed());

	{
		// Create Table that is missing the _fivetran_deleted column
		::fivetran_sdk::v2::CreateTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.mutable_table()->set_name(table_name);
		auto col1 = request.mutable_table()->add_columns();
		col1->set_name("something");
		col1->set_type(::fivetran_sdk::v2::DataType::STRING);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Attempt to truncate the table using a nonexisting _fivetran_deleted
		// column
		::fivetran_sdk::v2::TruncateRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);
		request.set_synced_column("_fivetran_synced"); // also does not exist although that does not
		                                               // matter
		request.mutable_soft()->set_deleted_column("_fivetran_deleted");

		const auto cutoff_datetime = 1707436800; // 2024-02-09 0:0:0 GMT, trust me
		request.mutable_utc_delete_before()->set_seconds(cutoff_datetime);
		request.mutable_utc_delete_before()->set_nanos(0);
		::fivetran_sdk::v2::TruncateResponse response;
		auto status = service.Truncate(nullptr, &request, &response);
		REQUIRE_FALSE(status.ok());
		CHECK_THAT(status.error_message(), Catch::Matchers::ContainsSubstring(
		                                       "Referenced column \"_fivetran_synced\" not found in FROM clause"));
	}
}

TEST_CASE("Capabilities", "[integration]") {
	DestinationSdkImpl service;
	::fivetran_sdk::v2::CapabilitiesRequest request;
	::fivetran_sdk::v2::CapabilitiesResponse response;

	auto status = service.Capabilities(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);

	REQUIRE(response.batch_file_format() == ::fivetran_sdk::v2::CSV);
}

TEST_CASE("WriteHistoryBatch with update files", "[integration][write-batch]") {
	DestinationSdkImpl service;

	// Schema will be main
	const std::string table_name = "books" + std::to_string(Catch::rngSeed());

	{
		// Create Table
		::fivetran_sdk::v2::CreateTableRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		define_history_test_table(request, table_name);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// upsert some data, so that delete-earliest has something to delete
		::fivetran_sdk::v2::WriteBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		define_history_test_table(request, table_name);

		request.mutable_file_params()->set_null_string("magic-nullvalue");
		request.add_replace_files(TEST_RESOURCES_DIR + "books_history_upsert_regular_write.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// a WriteHistoryBatchRequest with only the earliest files provided.
		// (not a realistic scenario, but useful to isolate changes and test
		// idempotence in the next section)
		::fivetran_sdk::v2::WriteHistoryBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		define_history_test_table(request, table_name);

		request.add_earliest_start_files(TEST_RESOURCES_DIR + "books_history_earliest.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteHistoryBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	auto con = get_test_connection(MD_TOKEN);
	{
		// check that id=2 ("The Two Towers") got deleted because it's newer than
		// the date in books_history_earliest.csv
		auto res = con->Query("SELECT id, title, magic_number, _fivetran_deleted, _fivetran_synced, "
		                      "_fivetran_active, _fivetran_start, _fivetran_end"
		                      " FROM " +
		                      table_name + " ORDER BY id, _fivetran_start");
		REQUIRE_NO_FAIL(res);

		REQUIRE(res->RowCount() == 3);

		// old outdated record, not affected at all
		REQUIRE(res->GetValue(0, 0) == 3);
		REQUIRE(res->GetValue(1, 0) == "The old Hobbit");
		REQUIRE(res->GetValue(2, 0) == 100);
		REQUIRE(res->GetValue(3, 0) == false);                           // deleted
		REQUIRE(res->GetValue(4, 0) == "2023-01-09 04:10:19.156057+00"); // synced
		REQUIRE(res->GetValue(5, 0) == false);                           // no longer active
		REQUIRE(res->GetValue(6, 0) == "2023-01-09 04:10:19.156057+00"); // _fivetran_start
		REQUIRE(res->GetValue(7, 0) == "2024-01-09 04:10:19.155957+00"); // _fivetran_end

		// latest record as of right before this WriteHistoryBatch; should get
		// deactivated
		REQUIRE(res->GetValue(0, 1) == 3);
		REQUIRE(res->GetValue(1, 1) == "The Hobbit");
		REQUIRE(res->GetValue(2, 1) == 14);
		REQUIRE(res->GetValue(3, 1) == false);                           // deleted
		REQUIRE(res->GetValue(4, 1) == "2024-01-09 04:10:19.156057+00"); // synced
		REQUIRE(res->GetValue(5, 1) == false);                           // no longer active
		REQUIRE(res->GetValue(6, 1) == "2024-01-09 04:10:19.156057+00"); // _fivetran_start
		REQUIRE(res->GetValue(7, 1) == "2025-01-01 20:56:59.999+00");    // _fivetran_end updated to 1ms
		                                                                 // before the earliest

		// active record that's not part of the batch; not affected at all
		REQUIRE(res->GetValue(0, 2) == 99);
		REQUIRE(res->GetValue(5, 2) == true);                            // this primary key was not in the
		                                                                 // incoming batch, so not deactivated
		REQUIRE(res->GetValue(6, 2) == "2025-01-09 04:10:19.156057+00"); // _fivetran_start, no change
		REQUIRE(res->GetValue(7, 2) == "9999-01-09 04:10:19.156057+00"); // _fivetran_end, no change
	}

	{
		// a WriteHistoryBatchRequest with the same earliest files as before, plus
		// update files
		::fivetran_sdk::v2::WriteHistoryBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		// TBD: check what happens when unmodified string is not set - it seems to
		// be blank but shoudl it fail instead?
		request.mutable_file_params()->set_unmodified_string("unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
		request.mutable_file_params()->set_null_string("magic-nullvalue");

		define_history_test_table(request, table_name);

		request.add_earliest_start_files(TEST_RESOURCES_DIR + "books_history_earliest.csv");
		request.add_update_files(TEST_RESOURCES_DIR + "books_history_update.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteHistoryBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// check that id=2 ("The Two Towers") got deleted because it's newer than
		// the date in books_history_earliest.csv
		auto res = con->Query("SELECT id, title, magic_number, _fivetran_deleted, _fivetran_synced, "
		                      "_fivetran_active, _fivetran_start, _fivetran_end"
		                      " FROM " +
		                      table_name + " ORDER BY id, _fivetran_start");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 5);

		// new record for id=2
		REQUIRE(res->GetValue(0, 0) == 2);
		REQUIRE(res->GetValue(1, 0) == "The empire strikes back");
		REQUIRE(res->GetValue(2, 0).IsNull());                    // there was no previous record, so no previous value
		REQUIRE(res->GetValue(3, 0) == false);                    // deleted
		REQUIRE(res->GetValue(4, 0) == "2025-02-08 12:00:00+00"); // synced
		REQUIRE(res->GetValue(5, 0) == true);                     // active, as per file
		REQUIRE(res->GetValue(6, 0) == "2025-02-08 12:00:00+00"); // _fivetran_start
		REQUIRE(res->GetValue(7, 0) == "9999-02-08 12:00:00+00"); // _fivetran_end

		// old outdated record, not affected at all
		REQUIRE(res->GetValue(0, 1) == 3);
		REQUIRE(res->GetValue(1, 1) == "The old Hobbit");
		REQUIRE(res->GetValue(2, 1) == 100);
		REQUIRE(res->GetValue(3, 1) == false);                           // deleted
		REQUIRE(res->GetValue(4, 1) == "2023-01-09 04:10:19.156057+00"); // synced
		REQUIRE(res->GetValue(5, 1) == false);                           // no longer active
		REQUIRE(res->GetValue(6, 1) == "2023-01-09 04:10:19.156057+00"); // _fivetran_start
		REQUIRE(res->GetValue(7, 1) == "2024-01-09 04:10:19.155957+00"); // _fivetran_end

		// no change in the historical record from the last check
		REQUIRE(res->GetValue(0, 2) == 3);
		REQUIRE(res->GetValue(1, 2) == "The Hobbit");
		REQUIRE(res->GetValue(2, 2) == 14);
		REQUIRE(res->GetValue(3, 2) == false);                           // deleted
		REQUIRE(res->GetValue(4, 2) == "2024-01-09 04:10:19.156057+00"); // synced
		REQUIRE(res->GetValue(5, 2) == false);                           // no longer active
		REQUIRE(res->GetValue(6, 2) == "2024-01-09 04:10:19.156057+00"); // _fivetran_start
		REQUIRE(res->GetValue(7, 2) == "2025-01-01 20:56:59.999+00");    // _fivetran_end updated to 1ms
		                                                                 // before the earliest

		// new version of an existing record
		REQUIRE(res->GetValue(0, 3) == 3);
		// unmodified value came through from previous version;
		// did not accidentally pick up "The old Hobbit" from an older record
		REQUIRE(res->GetValue(1, 3) == "The Hobbit");
		REQUIRE(res->GetValue(2, 3) == 123);
		REQUIRE(res->GetValue(3, 3) == false);                    // deleted
		REQUIRE(res->GetValue(4, 3) == "2025-02-08 12:00:00+00"); // synced
		REQUIRE(res->GetValue(5, 3) == true);                     // new version active, per file
		REQUIRE(res->GetValue(6, 3) == "2025-02-08 12:00:00+00"); // _fivetran_start
		REQUIRE(res->GetValue(7, 3) == "9999-02-08 12:00:00+00"); // _fivetran_end updated to 1ms before
		                                                          // the earliest

		// no change in the historical record from the last check
		REQUIRE(res->GetValue(0, 4) == 99);
		REQUIRE(res->GetValue(5, 4) == true);                            // this primary key was not in the
		                                                                 // incoming batch, so not deactivated
		REQUIRE(res->GetValue(6, 4) == "2025-01-09 04:10:19.156057+00"); // _fivetran_start, no change
		REQUIRE(res->GetValue(7, 4) == "9999-01-09 04:10:19.156057+00"); // _fivetran_end, no change
	}
}

TEST_CASE("WriteHistoryBatch upsert and delete", "[integration][write-batch]") {
	DestinationSdkImpl service;

	// Schema will be main
	const std::string table_name = "books" + std::to_string(Catch::rngSeed());

	{
		// Create Table
		::fivetran_sdk::v2::CreateTableRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		define_history_test_table(request, table_name);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// history write with the earliest file (that does not affect anything
		// because there is no data), plus upsert file
		::fivetran_sdk::v2::WriteHistoryBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		request.mutable_file_params()->set_unmodified_string("unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
		request.mutable_file_params()->set_null_string("magic-nullvalue");

		define_history_test_table(request, table_name);

		request.add_earliest_start_files(TEST_RESOURCES_DIR + "books_history_earliest.csv");
		request.add_replace_files(TEST_RESOURCES_DIR + "books_history_upsert.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteHistoryBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	auto con = get_test_connection(MD_TOKEN);
	{
		auto res = con->Query("SELECT id, title, magic_number, _fivetran_deleted, _fivetran_synced, "
		                      "_fivetran_active, _fivetran_start, _fivetran_end"
		                      " FROM " +
		                      table_name + " ORDER BY id, _fivetran_start");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 1);

		// record inserted as is
		REQUIRE(res->GetValue(0, 0) == 3);
		REQUIRE(res->GetValue(1, 0) == "The Hobbit");
		REQUIRE(res->GetValue(2, 0) == 14);
		REQUIRE(res->GetValue(3, 0) == false);                           // deleted
		REQUIRE(res->GetValue(4, 0) == "2024-01-09 04:10:19.156057+00"); // synced
		REQUIRE(res->GetValue(5, 0) == true);                            // active, per file
		REQUIRE(res->GetValue(6, 0) == "2024-01-09 04:10:19.156057+00"); // _fivetran_start
		REQUIRE(res->GetValue(7, 0) == "9999-01-09 04:10:19.156057+00"); // _fivetran_end
	}

	{
		// history write with just the delete file (for testing; normally there
		// would also be the earliest start file)
		::fivetran_sdk::v2::WriteHistoryBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		request.mutable_file_params()->set_unmodified_string("unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
		request.mutable_file_params()->set_null_string("magic-nullvalue");

		define_history_test_table(request, table_name);

		request.add_delete_files(TEST_RESOURCES_DIR + "books_history_delete.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteHistoryBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// same as above (history write with delete file only), but this delete file
		// is missing _fivetran_start and _fivetran_active columns. This seems to be
		// the structure of the historical delete files that come through in real
		// life
		::fivetran_sdk::v2::WriteHistoryBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		request.mutable_file_params()->set_unmodified_string("unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
		request.mutable_file_params()->set_null_string("magic-nullvalue");

		define_history_test_table(request, table_name);

		request.add_delete_files(TEST_RESOURCES_DIR +
		                         "books_history_delete_fivetran_start_and_fivetran_active_missing.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteHistoryBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// same as above (history write with delete file only), but this delete file
		// has only the _fivetran_end column and the primary key in it
		::fivetran_sdk::v2::WriteHistoryBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		request.mutable_file_params()->set_unmodified_string("unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
		request.mutable_file_params()->set_null_string("magic-nullvalue");

		define_history_test_table(request, table_name);

		request.add_delete_files(TEST_RESOURCES_DIR + "books_history_delete_pk_only.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteHistoryBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// check that id=2 ("The Two Towers") got deleted because it's newer than
		// the date in books_history_earliest.csv
		auto res = con->Query("SELECT id, title, magic_number, _fivetran_deleted, _fivetran_synced, "
		                      "_fivetran_active, _fivetran_start, _fivetran_end"
		                      " FROM " +
		                      table_name + " ORDER BY id, _fivetran_start");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 1);

		// record inserted as is
		REQUIRE(res->GetValue(0, 0) == 3);
		REQUIRE(res->GetValue(1, 0) == "The Hobbit");
		REQUIRE(res->GetValue(2, 0) == 14);
		REQUIRE(res->GetValue(3, 0) == false);                           // deleted
		REQUIRE(res->GetValue(4, 0) == "2024-01-09 04:10:19.156057+00"); // synced
		REQUIRE(res->GetValue(5, 0) == false);                           // no longer active
		REQUIRE(res->GetValue(6, 0) == "2024-01-09 04:10:19.156057+00"); // _fivetran_start
		REQUIRE(res->GetValue(7, 0) == "2025-03-09 04:10:19.156057+00"); // _fivetran_end updated per
		                                                                 // delete file
	}
}

TEST_CASE("WriteHistoryBatch should delete overlapping records", "[integration][write-batch]") {
	DestinationSdkImpl service;

	const std::string table_name = "books" + std::to_string(Catch::rngSeed());

	{
		// Create Table
		::fivetran_sdk::v2::CreateTableRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		define_history_test_table(request, table_name);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Initial batch
		::fivetran_sdk::v2::WriteHistoryBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		request.mutable_file_params()->set_unmodified_string("unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
		request.mutable_file_params()->set_null_string("magic-nullvalue");

		define_history_test_table(request, table_name);

		request.add_earliest_start_files(TEST_RESOURCES_DIR + "books_history_earlier_earliest_1.csv");
		request.add_replace_files(TEST_RESOURCES_DIR + "books_history_earlier_upsert_1.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteHistoryBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Batch with overlapping value for id=2
		::fivetran_sdk::v2::WriteHistoryBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		request.mutable_file_params()->set_unmodified_string("unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
		request.mutable_file_params()->set_null_string("magic-nullvalue");

		define_history_test_table(request, table_name);

		request.add_earliest_start_files(TEST_RESOURCES_DIR + "books_history_earlier_earliest_2.csv");
		request.add_replace_files(TEST_RESOURCES_DIR + "books_history_earlier_upsert_2.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteHistoryBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		auto con = get_test_connection(MD_TOKEN);

		auto res = con->Query("SELECT title, _fivetran_start FROM " + table_name + " ORDER BY id");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 3);
		REQUIRE(res->GetValue(0, 1) == "The Two Towers Updated Title");
		REQUIRE(res->GetValue(1, 1) == "2024-01-01 04:10:19.156057+00");
	}
}

TEST_CASE("WriteBatch and WriteHistoryBatch with reordered CSV columns", "[integration][write-batch]") {
	// Test that history mode handles CSV files where columns are in a different
	// order than the table definition. This verifies that INSERT statements
	// use explicit column lists rather than relying on positional matching.
	DestinationSdkImpl service;

	const std::string table_name = "books_reordered" + std::to_string(Catch::rngSeed());

	{
		// Create Table with columns in a specific order:
		// id, title, magic_number, _fivetran_deleted, _fivetran_synced,
		// _fivetran_active, _fivetran_start, _fivetran_end
		::fivetran_sdk::v2::CreateTableRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		define_history_test_table(request, table_name);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Insert initial data using a CSV file where columns are in a DIFFERENT
		// order: _fivetran_end, magic_number, _fivetran_active, title,
		// _fivetran_synced, _fivetran_start, _fivetran_deleted, id
		::fivetran_sdk::v2::WriteBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		define_history_test_table_reordered(request, table_name);

		request.mutable_file_params()->set_null_string("magic-nullvalue");
		request.add_replace_files(TEST_RESOURCES_DIR + "history_reordered_initial.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	auto con = get_test_connection(MD_TOKEN);
	{
		// Verify initial data was inserted correctly despite column order mismatch
		auto res = con->Query("SELECT id, title, magic_number, _fivetran_deleted, _fivetran_active"
		                      " FROM " +
		                      table_name + " ORDER BY id");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 2);

		check_row(res, 0, {1, "Initial Book", 100, false, true});
		check_row(res, 1, {2, "Second Book", 200, false, true});
	}

	{
		// WriteHistoryBatch with update files - request columns in DIFFERENT order
		// than the table was created with. This tests that INSERT statements
		// use explicit column lists and don't rely on positional matching.
		::fivetran_sdk::v2::WriteHistoryBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		request.mutable_file_params()->set_unmodified_string("unmod-testmarker");
		request.mutable_file_params()->set_null_string("magic-nullvalue");

		// Use REORDERED column definition - different order than CreateTable used
		define_history_test_table_reordered(request, table_name);

		request.add_earliest_start_files(TEST_RESOURCES_DIR + "history_reordered_earliest.csv");
		request.add_update_files(TEST_RESOURCES_DIR + "history_reordered_update.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteHistoryBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Verify the update was applied correctly - id=1 should have new values,
		// id=2 should have preserved values from the unmodified marker
		auto res = con->Query("SELECT id, title, magic_number FROM " + table_name +
		                      " WHERE _fivetran_start >= '2025-03-01' ORDER BY id");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 2);

		check_row(res, 0, {1, "Updated Book", 999}); // updated
		check_row(res, 1, {2, "Second Book", 200});  // preserved via unmodified marker
	}
}

TEST_CASE("WriteBatch and WriteHistoryBatch with upsert", "[integration][write-batch]") {
	DestinationSdkImpl service;

	// Integration test that tries to mimic the destination tester on input_5.json
	// (with history mode) and creates two tables, of which one is a live table
	// and one is a historic table.

	// Schema will be main
	const std::string transaction_table_name = "transaction" + std::to_string(Catch::rngSeed());
	const std::string transaction_history_table_name = "transaction_history" + std::to_string(Catch::rngSeed());

	{
		// Create Tables
		::fivetran_sdk::v2::CreateTableRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		define_transaction_test_table(request, transaction_table_name);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);

		::fivetran_sdk::v2::CreateTableRequest request2;
		set_up_plain_write_request(request2, MD_TOKEN, TEST_DATABASE_NAME);
		define_transaction_history_test_table(request2, transaction_history_table_name);

		::fivetran_sdk::v2::CreateTableResponse response2;
		auto status2 = service.CreateTable(nullptr, &request2, &response2);
		REQUIRE_NO_FAIL(status2);
	}

	{
		// WriteBatch
		::fivetran_sdk::v2::WriteBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		request.mutable_file_params()->set_unmodified_string("unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
		request.mutable_file_params()->set_null_string("null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP");

		define_transaction_test_table(request, transaction_table_name);
		request.add_replace_files(TEST_RESOURCES_DIR + "transaction_input_5_upsert.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// WriteHistoryBatch
		// history write with the earliest file (that does not affect anything
		// because there is no data), plus upsert file
		::fivetran_sdk::v2::WriteHistoryBatchRequest request;
		set_up_plain_write_request(request, MD_TOKEN, TEST_DATABASE_NAME);
		request.mutable_file_params()->set_unmodified_string("unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
		request.mutable_file_params()->set_null_string("null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP");

		define_transaction_history_test_table(request, transaction_history_table_name);
		request.add_earliest_start_files(TEST_RESOURCES_DIR + "transaction_history_input_5_earliest.csv");
		request.add_replace_files(TEST_RESOURCES_DIR + "transaction_history_input_5_upsert.csv");

		::fivetran_sdk::v2::WriteBatchResponse response;
		auto status = service.WriteHistoryBatch(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	auto con = get_test_connection(MD_TOKEN);
	{
		auto res = con->Query("SELECT id, amount, \"desc\", _fivetran_synced, FROM " + transaction_table_name +
		                      " ORDER BY id");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 6);

		// All record ids present
		REQUIRE(res->GetValue(0, 0) == 1);
		REQUIRE(res->GetValue(0, 1) == 2);
		REQUIRE(res->GetValue(0, 2) == 3);
		REQUIRE(res->GetValue(0, 3) == 4);
		REQUIRE(res->GetValue(0, 4) == 10);
		REQUIRE(res->GetValue(0, 5) == 20);

		REQUIRE(res->GetValue(2, 0).IsNull()); // desc is null for row 0

		// As we see in the next test below, row 4 (id=10) is the only row with two
		// records in transaction_history. We pick the same entity to test here,
		// which at least makes sure that WriteBatch and WriteHistoryBatch do not
		// intervene in this test case, but otherwise this choice is arbitrary.
		REQUIRE(res->GetValue(1, 4) == 200);
		REQUIRE(res->GetValue(2, 4) == "three");
		REQUIRE(res->GetValue(3, 4) == "2025-12-17 12:30:40.937+00");
	}

	{
		// check that id=2 ("The Two Towers") got deleted because it's newer than
		// the date in books_history_earliest.csv
		auto res = con->Query("SELECT id, amount, _fivetran_active"
		                      " FROM " +
		                      transaction_history_table_name + " ORDER BY id, _fivetran_start");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 7);

		REQUIRE(res->GetValue(0, 0) == 1);
		REQUIRE(res->GetValue(0, 1) == 2);
		REQUIRE(res->GetValue(0, 2) == 3);
		REQUIRE(res->GetValue(0, 3) == 4);
		REQUIRE(res->GetValue(0, 4) == 10);
		REQUIRE(res->GetValue(0, 5) == 10);
		REQUIRE(res->GetValue(0, 6) == 20);

		// Item with id 10 gets an update of the amount column, which goes from 200
		// to 100.
		REQUIRE_FALSE(res->GetValue(2, 4) == true);
		REQUIRE(res->GetValue(2, 5) == true);

		REQUIRE(res->GetValue(1, 4) == 200);
		REQUIRE(res->GetValue(1, 5) == 100);
	}
}

TEST_CASE("AlterTable must not drop columns unless specified", "[integration]") {
	DestinationSdkImpl service;

	const std::string table_name = "some_table" + std::to_string(Catch::rngSeed());

	auto con = get_test_connection(MD_TOKEN);

	{
		// Create Table
		::fivetran_sdk::v2::CreateTableRequest request;
		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, false);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Alter Table to drop a regular column -- no-op because columns must not be
		// deleted
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
		request.set_drop_columns(false);
		// the second column is missing, but it should be retained

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	auto verifyTableStructure = [&](bool id_is_primary_key) {
		// Describe the altered table
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE(!response.not_found());

		REQUIRE(response.table().name() == table_name);
		REQUIRE(response.table().columns_size() == 2);
		REQUIRE(response.table().columns(0).name() == "id");
		REQUIRE(response.table().columns(0).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE(response.table().columns(0).primary_key() == id_is_primary_key);

		REQUIRE(response.table().columns(1).name() == "name");
		REQUIRE(response.table().columns(1).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE_FALSE(response.table().columns(1).primary_key());
	};

	verifyTableStructure(true);

	{
		// Alter Table to drop a primary key column -- no-op because columns must
		// not be deleted
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		// the first column is missing, but it should be retained
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, false);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	verifyTableStructure(true);

	{
		// Alter Table to change the type on a primary key column and drop the
		// regular column Still no-op but needs a separate test because changing
		// primary key status results in table recreation, so could accidentally
		// cause a column to be dropped
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING,
		        false); // primary key to regular column
		// the second column is missing, but it should be retained

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	verifyTableStructure(false); // "id" no longer a primary key; otherwise same

	{
		// Alter Table adding two columns and sending one of the two existing
		// columns with primary key changed This exercises the recreate path.
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		// one new column coming in before an existing column
		add_col(request, "new_before", ::fivetran_sdk::v2::DataType::STRING, false);
		// the first column is missing, but it should be retained
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, true);
		// one new column coming in after an existing column
		add_col(request, "new_after", ::fivetran_sdk::v2::DataType::STRING, false);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Describe the altered table
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE(!response.not_found());

		REQUIRE(response.table().name() == table_name);
		REQUIRE(response.table().columns_size() == 4);
		REQUIRE(response.table().columns(0).name() == "id");
		REQUIRE(response.table().columns(0).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE_FALSE(response.table().columns(0).primary_key());

		REQUIRE(response.table().columns(1).name() == "name");
		REQUIRE(response.table().columns(1).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE(response.table().columns(1).primary_key());

		// both new columns are added to the end, in the order they were requested
		REQUIRE(response.table().columns(2).name() == "new_before");
		REQUIRE(response.table().columns(2).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE_FALSE(response.table().columns(2).primary_key());

		REQUIRE(response.table().columns(3).name() == "new_after");
		REQUIRE(response.table().columns(3).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE_FALSE(response.table().columns(3).primary_key());
	}

	{
		// Alter Table adding two more columns and sending one of the two existing
		// columns unchanged This exercises the in-place path.
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		// one new column coming in before an existing column
		add_col(request, "new_before2", ::fivetran_sdk::v2::DataType::STRING, false);
		// the first column is missing, but it should be retained
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, true);
		// one new column coming in after an existing column
		add_col(request, "new_after2", ::fivetran_sdk::v2::DataType::STRING, false);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Describe the altered table
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE(!response.not_found());

		REQUIRE(response.table().name() == table_name);
		REQUIRE(response.table().columns_size() == 6);
		REQUIRE(response.table().columns(0).name() == "id");
		REQUIRE(response.table().columns(0).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE_FALSE(response.table().columns(0).primary_key());

		REQUIRE(response.table().columns(1).name() == "name");
		REQUIRE(response.table().columns(1).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE(response.table().columns(1).primary_key());

		REQUIRE(response.table().columns(2).name() == "new_before");
		REQUIRE(response.table().columns(2).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE_FALSE(response.table().columns(2).primary_key());

		REQUIRE(response.table().columns(3).name() == "new_after");
		REQUIRE(response.table().columns(3).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE_FALSE(response.table().columns(3).primary_key());

		// both new columns are added to the end, in the order they were requested
		REQUIRE(response.table().columns(4).name() == "new_before2");
		REQUIRE(response.table().columns(4).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE_FALSE(response.table().columns(4).primary_key());

		REQUIRE(response.table().columns(5).name() == "new_after2");
		REQUIRE(response.table().columns(5).type() == ::fivetran_sdk::v2::DataType::STRING);
		REQUIRE_FALSE(response.table().columns(5).primary_key());
	}
}

TEST_CASE("AlterTable must drop columns when specified", "[integration]") {
	DestinationSdkImpl service;

	const std::string table_name = "some_table" + std::to_string(Catch::rngSeed());

	auto con = get_test_connection(MD_TOKEN);

	{
		// Create Table
		::fivetran_sdk::v2::CreateTableRequest request;
		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, false);
		add_col(request, "test", ::fivetran_sdk::v2::DataType::STRING, false);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Alter Table to drop the name column
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
		add_col(request, "test", ::fivetran_sdk::v2::DataType::STRING, false);
		request.set_drop_columns(true);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		::fivetran_sdk::v2::DescribeTableRequest request;
		(*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
		(*request.mutable_configuration())["motherduck_database"] = TEST_DATABASE_NAME;
		request.set_table_name(table_name);

		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE(!response.not_found());

		REQUIRE(response.table().name() == table_name);
		REQUIRE(response.table().columns_size() == 2);
		REQUIRE(response.table().columns(0).name() == "id");
		REQUIRE(response.table().columns(1).name() == "test");
	}
}

TEST_CASE("AlterTable decimal width change", "[integration]") {
	DestinationSdkImpl service;

	const std::string table_name = "some_table" + std::to_string(Catch::rngSeed());

	auto con = get_test_connection(MD_TOKEN);

	auto verify_decimal_column = [&](uint32_t expected_precision, uint32_t expected_scale) {
		::fivetran_sdk::v2::DescribeTableRequest request;
		add_config(request, MD_TOKEN, TEST_DATABASE_NAME);
		request.set_table_name(table_name);

		::fivetran_sdk::v2::DescribeTableResponse response;
		auto status = service.DescribeTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
		REQUIRE(!response.not_found());

		REQUIRE(response.table().name() == table_name);
		REQUIRE(response.table().columns_size() == 2);

		REQUIRE(response.table().columns(0).name() == "id");
		REQUIRE(response.table().columns(0).type() == ::fivetran_sdk::v2::DataType::INT);

		REQUIRE(response.table().columns(1).name() == "amount");
		REQUIRE(response.table().columns(1).type() == ::fivetran_sdk::v2::DataType::DECIMAL);
		REQUIRE(response.table().columns(1).has_params());
		REQUIRE(response.table().columns(1).params().has_decimal());
		REQUIRE(response.table().columns(1).params().decimal().precision() == expected_precision);
		REQUIRE(response.table().columns(1).params().decimal().scale() == expected_scale);
	};

	auto verify_data = [&](const std::string& expected_amount) {
		auto res = con->Query("SELECT id, amount FROM " + table_name);
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->RowCount() == 1);
		REQUIRE(res->GetValue(0, 0).ToString() == "1");
		REQUIRE(res->GetValue(1, 0).ToString() == expected_amount);
	};

	{
		// Create Table with DECIMAL(17,4)
		::fivetran_sdk::v2::CreateTableRequest request;
		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
		add_decimal_col(request, "amount", false, 17, 4);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Insert test data
		auto res = con->Query("INSERT INTO " + table_name + " (id, amount) VALUES (1, 1234567890123.4567)");
		REQUIRE_NO_FAIL(res);
	}

	{
		INFO("Verifying initial DECIMAL(17,4)");
		verify_decimal_column(17, 4);
	}

	{
		INFO("Verifying initial data");
		verify_data("1234567890123.4567");
	}

	{
		// Alter Table to change DECIMAL(17,4) to DECIMAL(29,4)
		::fivetran_sdk::v2::AlterTableRequest request;
		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
		add_decimal_col(request, "amount", false, 29, 4);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		INFO("Verifying DECIMAL(29,4) after first alter with only the wider width");
		verify_decimal_column(29, 4);
	}
	{
		INFO("Verifying data preserved after first alter with only the wider width");
		verify_data("1234567890123.4567");
	}

	{
		// Alter Table to change DECIMAL(29,4) to DECIMAL(31,6)
		::fivetran_sdk::v2::AlterTableRequest request;
		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
		add_decimal_col(request, "amount", false, 31, 6);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		INFO("Verifying DECIMAL(31,6) after second alter to both wider width and "
		     "scale");
		verify_decimal_column(31, 6);
	}
	{
		INFO("Verifying data preserved after second alter to both wider width and "
		     "scale");
		verify_data("1234567890123.456700");
	}

	{
		// Return the table back to DECIMAL(17,4), which should fit because that's
		// where it started
		::fivetran_sdk::v2::AlterTableRequest request;
		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
		add_decimal_col(request, "amount", false, 17, 4);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		INFO("Verifying DECIMAL(17,4) after altering to narrower but still "
		     "sufficient width and scale");
		verify_decimal_column(17, 4);
	}

	{
		INFO("Verifying data preserved after altering to narrower but still "
		     "sufficient width and scale");
		verify_data("1234567890123.4567");
	}

	{
		// Attempt to shrink the type scale, which will succeed and round the
		// number. Even though the width shrinks, the whole part still fits; the
		// reduction comes from the fractional part
		::fivetran_sdk::v2::AlterTableRequest request;
		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
		add_decimal_col(request, "amount", false, 16, 3);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		INFO("Verifying DECIMAL(17,3) after altering to narrower scale");
		verify_decimal_column(16, 3);
	}

	{
		INFO("Verifying data gets rounded after altering to narrower scale");
		verify_data("1234567890123.457");
	}

	{
		// Attempt to shrink the type width, which will fail because the whole part
		// no longer fits
		::fivetran_sdk::v2::AlterTableRequest request;
		add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
		add_decimal_col(request, "amount", false, 15, 3);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_FALSE(status.ok());
		CHECK_THAT(status.error_message(), Catch::Matchers::ContainsSubstring("value is out of range"));
	}
}
