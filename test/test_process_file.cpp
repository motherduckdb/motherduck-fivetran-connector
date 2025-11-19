// #include "csv_processor.hpp"
//
#include "duckdb.hpp"
#include <catch2/catch_test_macros.hpp>
#include <filesystem>

namespace fs = std::filesystem;

TEST_CASE("CSVView drops database on cleanup") {
}

TEST_CASE("Test can read simple CSV file") {
  const fs::path test_file = fs::path("test_data") / "simple.csv";
  REQUIRE(fs::exists(test_file));

  // csv_processor::CSVView result_view = csv_processor::process_file();
}