#include "../integration/common.hpp"
#include "../constants.hpp"
#include "duckdb.hpp"

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <fstream>
#include <future>

using namespace test::constants;

std::unique_ptr<duckdb::Connection>
get_test_connection(const std::string &token) {
  duckdb::DBConfig config;
  config.SetOptionByName("motherduck_token", token);
  config.SetOptionByName("custom_user_agent", "fivetran-integration-test");
  duckdb::DuckDB db("md:" + TEST_DATABASE_NAME, &config);
  return std::make_unique<duckdb::Connection>(db);
}

// Helper to verify a row's values in order. Usage:
//   check_row(res, 0, {1, "Initial Book", 100, false});
void check_row(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &res,
               idx_t row, std::initializer_list<duckdb::Value> expected) {
  idx_t col = 0;
  for (const auto &val : expected) {
    REQUIRE(res->GetValue(col++, row) == val);
  }
}
