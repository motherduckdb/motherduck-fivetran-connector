#include "constants.hpp"
#include "duckdb.hpp"
#include "extension_helper.hpp"
#include <catch2/catch_session.hpp>

int main(const int argc, char *argv[]) {
  const auto token = std::getenv("motherduck_token");
  // We cannot use Catch2 macros outside a running session.
  // Hence, we throw a classic exception here.
  if (!token) {
    throw std::runtime_error(
        "Environment variable 'motherduck_token' is not set");
  }

  preload_extensions();
  // We create a new test database with randomized name 'fivetran_test_dbXYZ' at
  // the beginning of a test run.
  duckdb::DuckDB db;
  duckdb::Connection con(db);

  const auto load_res = con.Query("LOAD motherduck");
  if (load_res->HasError()) {
    throw std::runtime_error(
        "Could not load MotherDuck extension at test start: " +
        load_res->GetError());
  }

  const auto db_name = test::constants::TEST_DATABASE_NAME;
  // This query will establish the connection to MotherDuck.
  const auto create_res =
      con.Query("CREATE OR REPLACE DATABASE \"" + db_name + "\"");
  if (create_res->HasError()) {
    throw std::runtime_error("Could not create test database" + db_name +
                             " at test start: " + create_res->GetError());
  }

  const int result = Catch::Session().run(argc, argv);
  return result;
}
