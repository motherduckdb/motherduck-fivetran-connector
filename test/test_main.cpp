#include "constants.hpp"
#include "duckdb.hpp"
#include <catch2/catch_session.hpp>

int main(const int argc, char* argv[]) {
    const auto token = std::getenv("motherduck_token");
    // We cannot use Catch2 macros outside a running session.
    // Hence, we simply don't run the test setup here and let the
    // first test fail later if no token is provided.
    if (token) {
        // Clear database at the beginning of the test run
        duckdb::DuckDB db;
        duckdb::Connection con(db);
        con.Query("INSTALL motherduck");
        con.Query("LOAD motherduck");
        con.Query("CREATE OR REPLACE DATABASE \"" + TEST_DATABASE_NAME + "\"");
    }

    const int result = Catch::Session().run(argc, argv);
    return result;
}
