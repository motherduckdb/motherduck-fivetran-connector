#include <catch2/catch_test_macros.hpp>

#include "constants.hpp"
#include "duckdb.h"
#include "md_logging.hpp"

using namespace test::constants;

std::unique_ptr<duckdb::Connection>
copy_of_get_test_connection(const std::string &token) {
  // copied because with #121 this moves to a common test helpers file
  duckdb::DBConfig config;
  config.SetOptionByName("motherduck_token", token);
  config.SetOptionByName("custom_user_agent", "fivetran-integration-test");
  duckdb::DuckDB db("md:" + TEST_DATABASE_NAME, &config);
  return std::make_unique<duckdb::Connection>(db);
}

TEST_CASE("Test md_logger handles broken transactions", "[md_logger]") {
  auto con = copy_of_get_test_connection(MD_TOKEN);
  auto logger = mdlog::Logger::CreateMultiSinkLogger(con.get());

  con->BeginTransaction();
  con->Query("CREATE TABLE test(id int primary key, value int)");
  con->Query("INSERT INTO test VALUES (1, 1)");
  auto result = con->Query("INSERT INTO test VALUES (1, 1)");
  REQUIRE(result->HasError());
  logger.info("Initially fails to write to duckdb");
  REQUIRE(!con->HasActiveTransaction());
}
