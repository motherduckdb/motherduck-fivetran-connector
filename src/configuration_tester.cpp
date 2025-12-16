#include "configuration_tester.hpp"

#include "duckdb.hpp"
#include <array>
#include <string>
#include <unordered_map>

namespace configuration_tester {
    namespace {
        /// Checks that a simple connection to MotherDuck can be established and that the user is authenticated
        TestResult run_authentication_test(duckdb::Connection &con) {
            // This fetches the welcome pack
            const auto result = con.Query("PRAGMA MD_VERSION");
            if (result->HasError()) {
                // Errors thrown in the initialization function are very verbose. Example:
                // Invalid Input Error: Initialization function "motherduck_duckdb_cpp_init" from file "motherduck.duckdb_extension" threw an exception: "Failed to attach 'my_db': no database/share named 'my_db' found".
                // We are only interested in the last part.
                const std::string original_error = result->GetError();
                const std::string boilerplate = "Initialization function \"motherduck_duckdb_cpp_init\" from file";
                if (original_error.find(boilerplate) != std::string::npos) {
                    const std::string search_string = "threw an exception: ";
                    auto pos = original_error.find(search_string);
                    if (pos != std::string::npos) {
                        const std::string error_message = original_error.substr(pos + search_string.length());
                        return TestResult(false, "Connection to MotherDuck failed: " + error_message);
                    }
                }

                return TestResult(false, original_error);
            }
            return TestResult(true);
        }

        /// Checks that the selected database can be written to
        TestResult run_database_type_test(duckdb::Connection &con, const std::unordered_map<std::string, std::string> &config) {
            // TODO: Use MD_PROP_DATABASE
            const auto db_name_it = config.find("motherduck_database");
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

        /// Checks that the account/authentication token has write permissions
        TestResult run_write_rollback_test(duckdb::Connection &con, const std::unordered_map<std::string, std::string> &config) {
            // Test write permissions by creating a temp table, inserting data, and
            // rolling back
            const auto begin_res = con->Query("BEGIN TRANSACTION");
            if (begin_res->HasError()) {
                return TestResult(false, "Could not begin transaction: " +
                                         begin_res->GetError());
            }

            const auto db_name_it = config.find("motherduck_database");
            if (db_name_it == config.cend()) {
                return TestResult(false, "Database name not provided in configuration");
            }
            const std::string db_name = db_name_it->second;

            const auto table_name = duckdb::KeywordHelper::WriteQuoted(db_name, '"') + R"("main"."__fivetran_test_table_$cmFuZG9t$")";
            const auto create_res =
                con->Query("CREATE TABLE " + table_name + " (id INTEGER, "
                           "value VARCHAR)");
            if (create_res->HasError()) {
                return TestResult(false, "Could not create table \"" + table_name + "\": " +
                                         create_res->GetError());
            }

            const auto insert_res = con->Query(
                "INSERT INTO " + table_name + " VALUES (1, 'test_value')");
            if (insert_res->HasError()) {
                return TestResult(false, "Could not insert into table \"" + table_name + "\": " +
                                         insert_res->GetError());
            }

            const auto select_res =
                con->Query("SELECT COUNT(*) FROM _fivetran_test_table");
            if (select_res->HasError()) {
                return TestResult(false, "Could not read from table \"" + table_name + "\": " +
                                         select_res->GetError());
            }

            const auto row_count = select_res->GetValue(0, 0).GetValue<int64_t>();
            if (row_count != 1) {
                return TestResult(false,
                    "Expected 1 row in test table, got " +
                    std::to_string(row_count));
            }

            const auto rollback_res = con->Query("ROLLBACK");
            if (rollback_res->HasError()) {
                return TestResult(false, "Could not rollback transaction for table \"" + table_name + "\": " +
                                         rollback_res->GetError());
            }

            return TestResult(true);
        }
    } // namespace

    std::array<TestCase, 3> get_test_cases() {
        return {TestCase {CONFIG_TEST_NAME_AUTHENTICATE,
                    "Test Authentication"},
                TestCase{CONFIG_TEST_NAME_DATABASE_TYPE,
                         "Verify database is writeable and not a MotherDuck share"},
                TestCase{CONFIG_TEST_NAME_WRITE_ROLLBACK,
                         "Test write permissions"}
        };
    }

    TestResult run_test(const std::string &test_name,
                  duckdb::Connection &con,
                  const std::unordered_map<std::string, std::string> &config) {
        if (test_name == CONFIG_TEST_NAME_AUTHENTICATE) {
            return run_authentication_test(con);
        }
        if (test_name == CONFIG_TEST_NAME_DATABASE_TYPE) {
            return run_database_type_test(con, config);
        }
        if (test_name == CONFIG_TEST_NAME_WRITE_ROLLBACK) {
            return run_write_rollback_test(con, config);
        }
        throw std::runtime_error("Unknown test name: " + test_name);
    }
} // namespace configuration_tester