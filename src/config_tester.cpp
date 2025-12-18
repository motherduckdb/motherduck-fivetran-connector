#include "config_tester.hpp"

#include "config.hpp"
#include "duckdb.hpp"

#include <array>
#include <google/protobuf/map.h>
#include <string>

namespace config_tester {
namespace {
/// Checks that a simple connection to MotherDuck can be established and that
/// the user is authenticated
TestResult run_authentication_test(duckdb::Connection &con) {
  // The "actual" test happens in DestinationSdkImpl::Test when establishing the
  // connection. The authentication test runs first because they are executed in
  // the order they are set in the ConfigurationForm response.
  const auto result = con.Query("PRAGMA MD_VERSION");
  if (result->HasError()) {
    return TestResult(false, result->GetError());
  }
  return TestResult(true);
}

/// Checks that the selected database can be written to
TestResult run_database_type_test(
    duckdb::Connection &con,
    const google::protobuf::Map<std::string, std::string> &config) {
  const auto db_name = config::find_property(config, config::PROP_DATABASE);

  // We connect in single-attach mode. There is only one non-internal database
  // attached. Note that the database might not be part of the workspace, i.e.,
  // might not be attached on the server.
  const auto prepared_stmt = con.Prepare(
      "SELECT type FROM md_all_databases() WHERE is_attached AND alias = ?");
  if (prepared_stmt->HasError()) {
    return TestResult(false, "Failed to prepare database type query: " +
                                 prepared_stmt->GetError());
  }

  duckdb::vector<duckdb::Value> params = {duckdb::Value(db_name)};
  const auto result = prepared_stmt->Execute(params, false);
  if (result->HasError()) {
    return TestResult(false, "Failed to execute database type query: " +
                                 result->GetError());
  }

  assert(result->type == duckdb::QueryResultType::MATERIALIZED_RESULT);
  // We are allowed to do this cast because we disallow streaming results.
  const auto materialized_result =
      dynamic_cast<duckdb::MaterializedQueryResult *>(result.get());

  if (materialized_result->RowCount() == 0) {
    // This case is not possible because we connect in single-attach mode and
    // the database must exist.
    return TestResult(false, "Database \"" + db_name +
                                 "\" not found. Please create the database "
                                 "first in your MotherDuck account.");
  }

  if (materialized_result->RowCount() > 1) {
    // This should not be possible with MotherDuck
    return TestResult(false, "Multiple databases found with alias \"" +
                                 db_name + "\"");
  }

  assert(materialized_result->ColumnCount() == 1);
  const auto db_type = materialized_result->GetValue(0, 0).ToString();
  if (db_type == "motherduck share") {
    return TestResult(false,
                      "Catalog \"" + db_name +
                          "\" is a read-only MotherDuck share. Please use a "
                          "writable database for Fivetran ingestion jobs.");
  }
  if (db_type.find("motherduck") == std::string::npos) {
    // We expect to run against type "motherduck" or "motherduck <something>"
    // where "<something>" can e.g. be "ducklake"
    return TestResult(
        false,
        "\"" + db_name + "\" is not a MotherDuck database, but has type \"" +
            db_type + "\". Please use a writable MotherDuck database instead.");
  }

  return TestResult(true);
}

/// Checks that the account/authentication token has write permissions
TestResult run_write_rollback_test(
    duckdb::Connection &con,
    const google::protobuf::Map<std::string, std::string> &config) {
  // Test write permissions by creating a table, inserting data, and rolling
  // back
  const auto begin_res = con.Query("BEGIN TRANSACTION");
  if (begin_res->HasError()) {
    return TestResult(false,
                      "Could not begin transaction: " + begin_res->GetError());
  }

  const auto db_name = config::find_property(config, config::PROP_DATABASE);
  const std::string schema_name =
      duckdb::KeywordHelper::WriteQuoted(db_name, '"') +
      ".\"_md_fivetran_test\"";
  const auto create_schema_res =
      con.Query("CREATE SCHEMA IF NOT EXISTS " + schema_name);
  if (create_schema_res->HasError()) {
    return TestResult(false, "Could not create schema \"" + schema_name +
                                 "\": " + create_schema_res->GetError());
  }

  const auto table_name = schema_name + ".\"test_table_$cmFuZG9t$\"";
  const auto create_table_res =
      con.Query("CREATE TABLE IF NOT EXISTS " + table_name +
                " (id INTEGER, value VARCHAR)");
  if (create_table_res->HasError()) {
    return TestResult(false, "Could not create table \"" + table_name +
                                 "\": " + create_table_res->GetError());
  }

  const auto insert_res =
      con.Query("INSERT INTO " + table_name + " VALUES (1, 'test_value')");
  if (insert_res->HasError()) {
    return TestResult(false, "Could not insert into table \"" + table_name +
                                 "\": " + insert_res->GetError());
  }

  const auto select_res = con.Query("SELECT COUNT(*) FROM " + table_name);
  if (select_res->HasError()) {
    return TestResult(false, "Could not read from table \"" + table_name +
                                 "\": " + select_res->GetError());
  }

  const auto row_count = select_res->GetValue(0, 0).GetValue<int64_t>();
  if (row_count != 1) {
    return TestResult(false, "Expected 1 row in test table, got " +
                                 std::to_string(row_count));
  }

  const auto rollback_res = con.Query("ROLLBACK");
  if (rollback_res->HasError()) {
    return TestResult(false, "Could not rollback transaction for table \"" +
                                 table_name +
                                 "\": " + rollback_res->GetError());
  }

  return TestResult(true);
}
} // namespace

std::array<TestCase, 3> get_test_cases() {
  return {TestCase{TEST_AUTHENTICATE, "Test that user is authenticated"},
          TestCase{TEST_DATABASE_TYPE, "Test that database is not read-only"},
          TestCase{TEST_WRITE_ROLLBACK, "Test write permissions to database"}};
}

TestResult
run_test(const std::string &test_name, duckdb::Connection &con,
         const google::protobuf::Map<std::string, std::string> &config) {
  if (test_name == TEST_AUTHENTICATE) {
    return run_authentication_test(con);
  }
  if (test_name == TEST_DATABASE_TYPE) {
    return run_database_type_test(con, config);
  }
  if (test_name == TEST_WRITE_ROLLBACK) {
    return run_write_rollback_test(con, config);
  }
  throw std::runtime_error("Unknown test name: " + test_name);
}
} // namespace config_tester