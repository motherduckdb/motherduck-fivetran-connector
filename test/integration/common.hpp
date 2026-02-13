#pragma once

#include <array>
#include <string>
#include "../constants.hpp"
#include "duckdb.hpp"
#include "fivetran_duckdb_interop.hpp"
#include "motherduck_destination_server.hpp"
#include "schema_types.hpp"
#include "extension_helper.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>


inline bool NO_FAIL(
    const duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result) {
  if (result->HasError()) {
    fprintf(stderr, "Query failed with message: %s\n",
            result->GetError().c_str());
  }
  return !result->HasError();
}

inline bool NO_FAIL(const grpc::Status &status) {
  if (!status.ok()) {
    UNSCOPED_INFO("Query failed with message: " + status.error_message());
  }
  return status.ok();
}

inline bool REQUIRE_FAIL(const grpc::Status &status,
                  const std::string &expected_error) {
  if (!status.ok()) {
    REQUIRE(status.error_message() == expected_error);
    return true;
  }
  return false;
}

inline bool REQUIRE_FAIL(const grpc::Status &status,
                  const Catch::Matchers::StringMatcherBase &matcher) {
  if (!status.ok()) {
    REQUIRE_THAT(status.error_message(), matcher);
    return true;
  }
  return false;
}

#define REQUIRE_NO_FAIL(result) REQUIRE(NO_FAIL((result)))

class testRunListener : public Catch::EventListenerBase {
public:
  using EventListenerBase::EventListenerBase;

  void testRunStarting(Catch::TestRunInfo const &) override {
    preload_extensions();
  }
};

CATCH_REGISTER_LISTENER(testRunListener)

std::unique_ptr<duckdb::Connection> get_test_connection(const std::string &token);

void check_row(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &res,
               idx_t row, std::initializer_list<duckdb::Value> expected);


template <typename T>
void add_col(T &request, const std::string &name,
			 ::fivetran_sdk::v2::DataType type, bool is_primary_key) {
	auto col = request.mutable_table()->add_columns();
	col->set_name(name);
	col->set_type(type);
	col->set_primary_key(is_primary_key);
}


constexpr std::array TEST_COLUMNS = {
	column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	column_def {.name = "title", .type = duckdb::LogicalTypeId::VARCHAR},
	column_def {.name = "magic_number", .type = duckdb::LogicalTypeId::INTEGER},
	column_def {.name = "_fivetran_deleted", .type = duckdb::LogicalTypeId::BOOLEAN},
	column_def {.name = "_fivetran_synced", .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
};

template <typename T>
void define_test_table(T &request, const std::string &table_name) {
	request.mutable_table()->set_name(table_name);
	for (auto column : TEST_COLUMNS) {
		add_col(request, column.name, get_fivetran_type(column.type), column.primary_key);
	}
}

constexpr std::array HISTORY_TEST_COLUMNS = {
	column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	column_def {.name = "title", .type = duckdb::LogicalTypeId::VARCHAR},
	column_def {.name = "magic_number", .type = duckdb::LogicalTypeId::INTEGER},
	column_def {.name = "_fivetran_synced", .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
	column_def {.name = "_fivetran_active", .type = duckdb::LogicalTypeId::BOOLEAN},
	column_def {.name = "_fivetran_start", .type = duckdb::LogicalTypeId::TIMESTAMP_TZ, .primary_key = true},
	column_def {.name = "_fivetran_end", .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
};

template <typename T>
void define_history_test_table(T &request, const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
	for (auto column : HISTORY_TEST_COLUMNS) {
		add_col(request, column.name, get_fivetran_type(column.type), column.primary_key);
	}
}

template <typename T>
void define_transaction_test_table(T &request, const std::string &table_name) {
    request.mutable_table()->set_name(table_name);
	add_col(request, "amount", ::fivetran_sdk::v2::DataType::INT, false);
	add_col(request, "_fivetran_synced", ::fivetran_sdk::v2::DataType::UTC_DATETIME, false);
	add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
	add_col(request, "desc", ::fivetran_sdk::v2::DataType::STRING, false);
}

template <typename T>
void define_transaction_history_test_table(T &request,
                                           const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
	add_col(request, "amount",::fivetran_sdk::v2::DataType::INT, false);
	add_col(request, "_fivetran_synced",::fivetran_sdk::v2::DataType::UTC_DATETIME, false);
	add_col(request, "_fivetran_end",::fivetran_sdk::v2::DataType::UTC_DATETIME, false);
	add_col(request, "_fivetran_active",::fivetran_sdk::v2::DataType::BOOLEAN, false);
	add_col(request, "desc",::fivetran_sdk::v2::DataType::STRING, false);
	add_col(request, "_fivetran_start",::fivetran_sdk::v2::DataType::UTC_DATETIME, true);
	add_col(request, "id",::fivetran_sdk::v2::DataType::INT, true);
}

template <typename T>
void define_test_multikey_table(T &request, const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
	add_col(request, "id1",::fivetran_sdk::v2::DataType::INT, true);
	add_col(request, "id2",::fivetran_sdk::v2::DataType::INT, true);
	add_col(request, "text",::fivetran_sdk::v2::DataType::STRING, false);
	add_col(request, "_fivetran_deleted",::fivetran_sdk::v2::DataType::BOOLEAN, false);
	add_col(request, "_fivetran_synced",::fivetran_sdk::v2::DataType::UTC_DATETIME, false);
}

/* compression and encryption are off/none by default */

template <typename T>
void add_config(T &request, const std::string &token,
                const std::string &database, const std::string &table) {
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] = database;
  request.mutable_table()->set_name(table);
}

inline void add_config(fivetran_sdk::v2::MigrateRequest &request, const std::string &token,
                       const std::string &database, const std::string &table) {
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] = database;
  request.mutable_details()->set_table(table);
}

inline void add_config(fivetran_sdk::v2::DescribeTableRequest &request, const std::string &token,
					   const std::string &database, const std::string &table) {
	(*request.mutable_configuration())["motherduck_token"] = token;
	(*request.mutable_configuration())["motherduck_database"] = database;
	request.set_table_name(table);
}

inline void add_config(fivetran_sdk::v2::TruncateRequest &request, const std::string &token,
					   const std::string &database, const std::string &table) {
	(*request.mutable_configuration())["motherduck_token"] = token;
	(*request.mutable_configuration())["motherduck_database"] = database;
	request.set_table_name(table);
}

template <typename T>
void add_config(T &request, const std::string &token, const std::string &database) {
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] = database;
}


template <typename T>
void add_decimal_col(T &request, const std::string &name, bool is_primary_key,
                     std::uint32_t precision, std::uint32_t scale) {
  auto col = request.mutable_table()->add_columns();
  col->set_name(name);
  col->set_type(::fivetran_sdk::v2::DataType::DECIMAL);
  col->set_primary_key(is_primary_key);
  col->mutable_params()->mutable_decimal()->set_precision(precision);
  col->mutable_params()->mutable_decimal()->set_scale(scale);
}

// Same columns as define_history_test_table but in a DIFFERENT order.
// Used to test that INSERT statements use explicit column lists.
template <typename T>
void define_history_test_table_reordered(T &request,
                                         const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
  add_col(request, "_fivetran_end", ::fivetran_sdk::v2::DataType::UTC_DATETIME, false);
  add_col(request, "magic_number", ::fivetran_sdk::v2::DataType::INT, false);
  add_col(request, "_fivetran_active", ::fivetran_sdk::v2::DataType::BOOLEAN, false);
  add_col(request, "title", ::fivetran_sdk::v2::DataType::STRING, false);
  add_col(request, "_fivetran_synced", ::fivetran_sdk::v2::DataType::UTC_DATETIME, false);
  add_col(request, "_fivetran_start", ::fivetran_sdk::v2::DataType::UTC_DATETIME, true);
  add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
}

template <std::size_t N>
void create_table(DestinationSdkImpl &service, const std::string &table_name,
                  const std::array<column_def, N> columns) {
  ::fivetran_sdk::v2::CreateTableRequest request;
  add_config(request, test::constants::MD_TOKEN, test::constants::TEST_DATABASE_NAME, table_name);
  for (auto column : columns) {
  	if (column.type == duckdb::LogicalTypeId::DECIMAL && column.width > 0) {
  		add_decimal_col(request, column.name, column.primary_key, column.width, column.scale);
  	} else {
		add_col(request, column.name, get_fivetran_type(column.type), column.primary_key);
  	}
  }

  ::fivetran_sdk::v2::CreateTableResponse response;
  auto status = service.CreateTable(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);
  REQUIRE(response.success());
}

void create_table_basic(DestinationSdkImpl &service,
                        const std::string &table_name);

void create_table_with_varchar_col(DestinationSdkImpl &service,
                                   const std::string &table_name,
                                   const std::string &col_name);

void create_test_table(DestinationSdkImpl &service,
								   const std::string &table_name);

void create_history_table(DestinationSdkImpl &service,
                                   const std::string &table_name);

void check_column(const fivetran_sdk::v2::DescribeTableResponse &response,
                  int index, const std::string &name,
                  fivetran_sdk::v2::DataType type, bool primary_key);

fivetran_sdk::v2::DescribeTableResponse describe_table(
    DestinationSdkImpl &service, const std::string &table_name,
    const std::string &schema_name = "");
