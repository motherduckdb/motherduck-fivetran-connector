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
TestResult run_database_type_test(duckdb::Connection &con) {
  const auto current_db_res = con.Query("SELECT current_database()");
  if (current_db_res->HasError()) {
    return TestResult(false, "Failed to retrieve current database name: " +
                                 current_db_res->GetError());
  }
  assert(current_db_res->RowCount() == 1 && current_db_res->ColumnCount() == 1);
  const auto db_name = current_db_res->GetValue(0, 0).ToString();

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
TestResult run_write_permissions_test(duckdb::Connection &con) {
  const auto duckling_id_res = con.Query("FROM __md_duckling_id()");
  if (duckling_id_res->HasError()) {
    return TestResult(false, "Failed to retrieve duckling ID: " +
                                 duckling_id_res->GetError());
  }
  assert(duckling_id_res->RowCount() == 1 &&
         duckling_id_res->ColumnCount() == 1);
  const auto duckling_id = duckling_id_res->GetValue(0, 0).ToString();

  // For read-scaling tokens, the duckling ID ends with ".rs.<duckling number>"
  // For read-write tokens, the duckling ID ends with ".rw".
  // If neither is the case, optimistically return success too not rely to much
  // on internals.

  if (duckling_id.ends_with(".rw")) {
    return TestResult(true);
  }

  const auto last_dot_pos = duckling_id.rfind('.');
  if (last_dot_pos == std::string::npos || last_dot_pos < 3) {
    return TestResult(true);
  }
  if (duckling_id.substr(last_dot_pos - 3, 3) == ".rs") {
    return TestResult(
        false,
        "The provided authentication token is a read-scaling token. A token "
        "with write permissions is required to ingest data from Fivetran.");
  }

  return TestResult(true);
}
} // namespace

std::array<TestCase, 3> get_test_cases() {
  return {TestCase{TEST_AUTHENTICATE, "Test that user is authenticated"},
          TestCase{TEST_DATABASE_TYPE, "Test that database is not read-only"},
          TestCase{TEST_WRITE_PERMISSIONS,
                   "Test that auth token has write permissions"}};
}

TestResult run_test(const std::string &test_name, duckdb::Connection &con) {
  if (test_name == TEST_AUTHENTICATE) {
    return run_authentication_test(con);
  }
  if (test_name == TEST_DATABASE_TYPE) {
    return run_database_type_test(con);
  }
  if (test_name == TEST_WRITE_PERMISSIONS) {
    return run_write_permissions_test(con);
  }
  throw std::runtime_error("Unknown test name: " + test_name);
}
} // namespace config_tester