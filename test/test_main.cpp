#include "constants.hpp"
#include "duckdb.hpp"
#include "extension_helper.hpp"

#include <catch2/catch_session.hpp>
#include <cstdlib>
#include <iostream>

int main(const int argc, char *argv[]) {
  // We cannot use Catch2 macros outside a running session.
  // Hence, we log to stderr and exit with non-zero code on failure.

  preload_extensions();

  const auto db_name = test::constants::TEST_DATABASE_NAME;
  {
    // We create a new test database with randomized name 'fivetran_test_dbXYZ'
    // at the beginning of a test run.
    duckdb::DuckDB db;
    duckdb::Connection con(db);
    const auto load_res = con.Query("LOAD motherduck");
    if (load_res->HasError()) {
      std::cerr << "Could not load MotherDuck extension at test start: "
                << load_res->GetError() << std::endl;
      exit(4);
    }

    assert(!test::constants::MD_TOKEN.empty());
    // This query will establish the connection to MotherDuck.
    const auto create_res =
        con.Query("CREATE OR REPLACE DATABASE \"" + db_name + "\"");
    if (create_res->HasError()) {
      std::cerr << "Could not create test database " << db_name
                << " at test start: " << create_res->GetError() << std::endl;
      exit(5);
    }
  }

  const int result = Catch::Session().run(argc, argv);

  {
    duckdb::DuckDB db;
    duckdb::Connection con(db);
    const auto load_res = con.Query("LOAD motherduck");
    // Ignore errors during cleanup.
    if (!load_res->HasError()) {
      con.Query("DROP DATABASE IF EXISTS \"" + db_name + "\" CASCADE");
    }
  }

  return result;
}
