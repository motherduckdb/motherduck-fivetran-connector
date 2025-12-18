#pragma once

#include "duckdb.hpp"
#include <array>
#include <cassert>
#include <string>

// Forward declaration to not have to include a protobuf header here
namespace google::protobuf {
template <typename Key, typename T> class Map;
}

namespace config_tester {
inline constexpr const char *TEST_AUTHENTICATE = "test_authentication";
inline constexpr const char *TEST_DATABASE_TYPE = "test_database_type";
inline constexpr const char *TEST_WRITE_ROLLBACK = "test_write_rollback";

struct TestCase {
  explicit TestCase(std::string name_, std::string description_)
      : name(std::move(name_)), description(std::move(description_)) {}

  std::string name;
  std::string description;
};

struct TestResult {
  explicit TestResult(const bool success_, std::string failure_message_ = "")
      : success(success_), failure_message(std::move(failure_message_)) {
    assert(success && failure_message.empty() ||
           !success && !failure_message.empty());
  }

  bool success;
  std::string failure_message;
};

std::array<TestCase, 3> get_test_cases();

TestResult
run_test(const std::string &test_name, duckdb::Connection &con,
         const google::protobuf::Map<std::string, std::string> &config);
} // namespace config_tester