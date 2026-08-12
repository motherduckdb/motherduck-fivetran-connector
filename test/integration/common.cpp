#include "../integration/common.hpp"

#include "../constants.hpp"
#include "duckdb.hpp"

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <fstream>
#include <future>

using namespace test::constants;

std::unique_ptr<duckdb::Connection> get_test_connection(const std::string& token) {
	duckdb::DBConfig config;
	config.SetOptionByName("motherduck_token", token);
	config.SetOptionByName("custom_user_agent", "fivetran-integration-test");
	duckdb::DuckDB db("md:" + TEST_DATABASE_NAME, &config);
	return std::make_unique<duckdb::Connection>(db);
}

void create_table_with_varchar_col(DestinationSdkImpl& service, const std::string& table_name,
                                   const std::string& col_name) {
	create_table(service, table_name,
	             std::array {
	                 column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	                 column_def {.name = col_name, .type = duckdb::LogicalTypeId::VARCHAR},
	             });
}

// Helper to verify a row's values in order. Usage:
//   check_row(res, 0, {1, "Initial Book", 100, false});
void check_row(duckdb::unique_ptr<duckdb::MaterializedQueryResult>& res, idx_t row,
               std::initializer_list<duckdb::Value> expected) {
	idx_t col = 0;
	for (const auto& val : expected) {
		if (val.IsNull()) {
			REQUIRE(res->GetValue(col++, row).IsNull());
		} else {
			REQUIRE(res->GetValue(col++, row) == val);
		}
	}
}

void check_column(const fivetran_sdk::v2::DescribeTableResponse& response, int index, const std::string& name,
                  fivetran_sdk::v2::DataType type, bool primary_key) {
	REQUIRE(response.table().columns(index).name() == name);
	REQUIRE(response.table().columns(index).type() == type);
	REQUIRE(response.table().columns(index).primary_key() == primary_key);
}

fivetran_sdk::v2::DescribeTableResponse describe_table(DestinationSdkImpl& service, const std::string& table_name,
                                                       const std::string& schema_name) {
	::fivetran_sdk::v2::DescribeTableRequest request;
	add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
	if (!schema_name.empty()) {
		request.set_schema_name(schema_name);
	}

	::fivetran_sdk::v2::DescribeTableResponse response;
	auto status = service.DescribeTable(nullptr, &request, &response);
	REQUIRE_NO_FAIL(status);
	return response;
}
