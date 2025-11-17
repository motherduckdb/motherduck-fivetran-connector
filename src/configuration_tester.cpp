#include "configuration_tester.hpp"

#include "duckdb.hpp"
#include <map>
#include <string>

namespace configuration_tester {
    namespace {
        TestResult run_csv_block_size_test(const std::map<std::string, std::string> &config) {
            // TODO: MD_PROP_CSV_BLOCK_SIZE
            const auto block_size_it = config.find("motherduck_csv_block_size");

            // CSV block size is optional, hence missing is fine
            if (block_size_it == config.cend()) {
                return TestResult(true);
            }

            // CSV block size must be numeric
            // TODO: Add bounds check
            // TODO: Add greater 0 check
            if (block_size_it->second.find_first_not_of("0123456789") != std::string::npos) {
                TestResult(false, "Maximum individual value size must be numeric if present");
            }

            return TestResult(true);
        }

        TestResult run_authentication_test(duckdb::Connection &con) {
            const auto result = con.Query("PRAGMA MD_VERSION");
            if (result->HasError()) {
                return TestResult(false, result->GetError());
            }
            return TestResult(true);
        }

        TestResult run_database_type_test(duckdb::Connection &con, const std::map<std::string, std::string> &config) {
            // TODO: MD_PROP_DATABASE
            auto db_name_it = config.find("motherduck_database");
            if (db_name_it == config.cend()) {
                return TestResult(false, "Database name not provided in configuration");
            }
            const std::string db_name = db_name_it->second;

            // We connect in single-attach mode. There is only one non-internal database attached.
            // Note that the database might not be part of the workspace, i.e., might not be attached on the server.
            const auto prepared_stmt = con.Prepare(
                "SELECT type FROM duckdb_databases() WHERE database_name = ? AND internal = false AND type <> 'motherduck_info'");
            if (prepared_stmt->HasError()) {
                return TestResult(false, "Failed to prepare database type query: " + prepared_stmt->GetError());
            }

            duckdb::vector<duckdb::Value> params = {duckdb::Value(db_name)};
            const auto result = prepared_stmt->Execute(params, false);
            if (result->HasError()) {
                return TestResult(false, "Failed to execute database type query: " + result->GetError());
            }

            assert(result->type == duckdb::QueryResultType::MATERIALIZED_RESULT);
            // We are allowed to do this cast because we disallow streaming results.
            const auto materialized_result = dynamic_cast<duckdb::MaterializedQueryResult *>(result.get());

            if (materialized_result->RowCount() == 0) {
                // This case is not possible because we connect in single-attach mode and the database must exist.
                // TODO: Would it be more user-friendly if we just created the database if it does not exist?
                return TestResult(false, "Database \"" + db_name + "\" not found. Please create the database first in your MotherDuck account.");
            }
            if (materialized_result->RowCount() > 1) {
                // This should not be possible with MotherDuck
                return TestResult(false, "Multiple databases found with alias \"" + db_name + "\"");
            }

            assert(materialized_result->ColumnCount() == 1);
            const auto db_type = materialized_result->GetValue(0, 0).ToString();
            if (db_type == "motherduck share") {
                return TestResult(false, "Catalog \"" + db_name +
                                        "\" is a read-only MotherDuck share. Please use a writable database for Fivetran ingestion jobs.");
            }
            if (db_type != "motherduck") {
                // TODO: In the future, we probably want to support other types as well, such as Ducklake.
                return TestResult(false, "\"" + db_name +
                                        "\" is not a MotherDuck database, but has type \"" + db_type + "\". Please use a writable MotherDuck database instead.");
            }

            return TestResult(true);
        }

        bool run_write_rollback_test() {
            return true;
        }
    }

    bool run_test(const std::string &test_name,
                  duckdb::Connection &con,
                  const std::map<std::string, std::string> &config) {
        if (test_name == CONFIG_TEST_NAME_AUTHENTICATE) {
            return run_authentication_test(con);
        }
        if (test_name == CONFIG_TEST_NAME_CSV_BLOCK_SIZE) {
            return run_csv_block_size_test(config);
        }
        if (test_name == CONFIG_TEST_NAME_DATABASE_TYPE) {
            return run_database_type_test();
        }
        if (test_name == CONFIG_TEST_NAME_WRITE_ROLLBACK) {
            return run_write_rollback_test();
        }
        throw std::runtime_error("Unknown test name: " + test_name);
    }
} // namespace configuration_tester