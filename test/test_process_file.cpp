#include "catch2/generators/catch_generators.hpp"
#include "catch2/matchers/catch_matchers.hpp"
#include "catch2/matchers/catch_matchers_string.hpp"
#include "csv_processor.hpp"
#include "duckdb.hpp"
#include <catch2/catch_test_macros.hpp>
#include <filesystem>

namespace fs = std::filesystem;

TEST_CASE("Test can read simple CSV file", "[csv_processor]") {
  const fs::path test_file =
      fs::path("test") / "files" / "csv" / "small_simple.csv";
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  IngestProperties props(test_file.string(), "", {}, "", 1);
  auto logger = std::make_shared<mdlog::MdLog>();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &view_name) {
        const std::string query = "FROM " + view_name;
        auto res = con.Query(query);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->ColumnCount() == 3);
        REQUIRE(res->RowCount() == 3);

        for (idx_t row = 0; row < res->RowCount(); row++) {
          auto id = res->GetValue(0, row).GetValue<int>();
          auto name = res->GetValue(1, row).ToString();
          auto age = res->GetValue(2, row).GetValue<int>();

          if (row == 0) {
            REQUIRE(id == 1);
            REQUIRE(name == "Alice");
            REQUIRE(age == 30);
          } else if (row == 1) {
            REQUIRE(id == 2);
            REQUIRE(name == "Bob");
            REQUIRE(age == 25);
          } else if (row == 2) {
            REQUIRE(id == 3);
            REQUIRE(name == "Charlie");
            REQUIRE(age == 35);
          }
        }
      });
}

TEST_CASE("Test reading various CSV files", "[csv_processor]") {
  const auto filename = GENERATE(
      "booleans", "dates_and_times", "large", "many_columns", "mixed_types",
      "nulls_and_empty", "numeric_types", "single_column", "special_chars");
  const fs::path test_file =
      fs::path("test") / "files" / "csv" / (std::string(filename) + ".csv");
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  CAPTURE(filename);

  IngestProperties props(test_file.string(), "", {}, "", 1);
  auto logger = std::make_shared<mdlog::MdLog>();
  csv_processor::ProcessFile(
      con, props, logger, [&con, &test_file](const std::string &view_name) {
        // Find the difference between the view and the original CSV data; should be empty
        const std::string compare_query =
            "WITH expected_data AS (FROM read_csv_auto('" + test_file.string() + "', header = true)) "
            "(FROM " + view_name + " EXCEPT FROM expected_data) "
            "UNION ALL "
            "(FROM expected_data EXCEPT FROM " + view_name + ")";
        const auto compare_result = con.Query(compare_query);
        if (compare_result->HasError()) {
          FAIL("Failed to execute comparison query: " +
               compare_result->GetError());
        }
        CHECK(compare_result->RowCount() == 0);
      });
}