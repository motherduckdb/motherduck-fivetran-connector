#include "constants.hpp"

#include <random>
#include <string>

namespace test::constants {
namespace {
std::string get_randomized_test_database_name() {
  const std::string TEST_DB_NAME_PREFIX = "fivetran_test_db";
  std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution dist(1, 9999);
  return TEST_DB_NAME_PREFIX + std::to_string(dist(gen));
}
} // namespace

const std::string TEST_DATABASE_NAME = get_randomized_test_database_name();
} // namespace test::constants