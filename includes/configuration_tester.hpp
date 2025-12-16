#pragma once

#include "duckdb.hpp"
#include <array>
#include <string>
#include <string_view>
#include <unordered_map>

namespace configuration_tester {
    constexpr std::string_view CONFIG_TEST_NAME_AUTHENTICATE =
        "test_authentication";
    constexpr std::string_view CONFIG_TEST_NAME_DATABASE_TYPE =
        "test_database_type";
    constexpr std::string_view CONFIG_TEST_NAME_WRITE_ROLLBACK =
        "test_write_rollback";

    struct TestCase {
        explicit TestCase(std::string name_, std::string description_)
            : name(std::move(name_)), description(std::move(description_)) {
        }

        std::string name;
        std::string description;
    };

    struct TestResult {
        explicit TestResult(const bool success_, std::string failure_message_ = "")
            : success(success_), failure_message(std::move(failure_message_)) {
            assert(success && failure_message.empty() || !success && !failure_message.empty());
        }

        bool success;
        std::string failure_message;
    };

    std::array<TestCase, 3> get_test_cases();

    TestResult run_test(const std::string &test_name,
                  duckdb::Connection &con,
                  const std::unordered_map<std::string, std::string> &config);
} // namespace configuration_tester