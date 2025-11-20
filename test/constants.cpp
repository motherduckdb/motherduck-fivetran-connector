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

std::string get_motherduck_auth_token() {
  char *token = std::getenv("motherduck_token");
  if (!token) {
    token = std::getenv("MOTHERDUCK_TOKEN");
  }
  if (!token) {
    throw std::runtime_error(
        "Environment variable \"motherduck_token\" is not set");
  }
  if (strlen(token) == 0) {
    throw std::runtime_error(
        "Environment variable \"motherduck_token\" is empty");
  }
  return std::string(token);
}
} // namespace

const std::string TEST_DATABASE_NAME = get_randomized_test_database_name();
const std::string MD_TOKEN = get_motherduck_auth_token();
} // namespace test::constants