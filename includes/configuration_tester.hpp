#pragma once

#include "duckdb.hpp"
#include <map>
#include <string>
#include <string_view>

namespace configuration_tester {
    constexpr std::string_view CONFIG_TEST_NAME_AUTHENTICATE =
        "test_authentication";
    constexpr std::string_view CONFIG_TEST_NAME_CSV_BLOCK_SIZE =
        "test_csv_block_size";
    constexpr std::string_view CONFIG_TEST_NAME_DATABASE_TYPE =
        "test_database_type";
    constexpr std::string_view CONFIG_TEST_NAME_WRITE_ROLLBACK =
        "test_write_rollback";

    struct TestResult {
        explicit TestResult(const bool success_, std::string failure_message_ = "")
            : success(success_), failure_message(std::move(failure_message_)) {
            assert(success && failure_message.empty() || !success && !failure_message.empty());
        }

        bool success;
        std::string failure_message;
    };

    bool run_test(const std::string &test_name,
                  duckdb::Connection &con,
                  const std::map<std::string, std::string> &config);
} // namespace configuration_tester