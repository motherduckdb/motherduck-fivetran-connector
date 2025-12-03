#include "constants.hpp"
#include "csv_processor.hpp"
#include "duckdb.hpp"
#include "types.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <filesystem>
#include <string>
#include <tuple>
#include <vector>

namespace fs = std::filesystem;
using namespace test::constants;

TEST_CASE("Test can read simple CSV file", "[csv_processor]") {
  const fs::path test_file =
      fs::path(TEST_RESOURCES_DIR) / "csv" / "small_simple.csv";
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

    std::vector<column_def> columns {
        column_def { .name = "id", .type = duckdb::LogicalType::INTEGER },
        column_def { .name = "name", .type = duckdb::LogicalType::VARCHAR },
        column_def { .name = "age", .type = duckdb::LogicalType::SMALLINT }
    };
  IngestProperties props(test_file.string(), "", columns, "", 1);
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

TEST_CASE("Test reading CSV file with auto-detection of column types", "[csv_processor]") {
    const fs::path test_file =
        fs::path(TEST_RESOURCES_DIR) / "csv" / "small_simple.csv";
    CAPTURE(test_file);
    REQUIRE(fs::exists(test_file));

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    std::vector<column_def> columns {
        column_def { .name = "id", .type = duckdb::LogicalType::INVALID },
        column_def { .name = "name", .type = duckdb::LogicalType::INVALID },
        column_def { .name = "age", .type = duckdb::LogicalType::INVALID }
    };
    IngestProperties props(test_file.string(), "", columns, "", 1);
    auto logger = std::make_shared<mdlog::MdLog>();
    csv_processor::ProcessFile(
        con, props, logger, [&con](const std::string &view_name) {
          const std::string query = "FROM " + view_name;
          auto res = con.Query(query);
          REQUIRE_FALSE(res->HasError());
          REQUIRE(res->ColumnCount() == 3);
          CHECK(res->RowCount() == 3);

            // CSV reader uses BIGINT for integer columns when auto-detecting.
            // This would get converted to the corresponding column type of a table during insertion.
            CHECK(res->types[0].id() == duckdb::LogicalTypeId::BIGINT);
            CHECK(res->types[1].id() == duckdb::LogicalTypeId::VARCHAR);
            CHECK(res->types[2].id() == duckdb::LogicalTypeId::BIGINT);
        });
}


// TODO: Test cases:
// - Test column order not matching
// - Test with different column data types (e.g. different int types). Also edge cases like overflow
// - null string and unmodified string handling
// - various error paths

TEST_CASE("Test reading various CSV files", "[csv_processor]") {
    auto [filename, row_count, columns] = GENERATE(table<std::string, size_t, std::vector<column_def>>({
        std::make_tuple<std::string, size_t, std::vector<column_def>>("booleans.csv", 6, {
            { .name = "id", .type = duckdb::LogicalType::INTEGER },
            { .name = "bool_true_false", .type = duckdb::LogicalType::BOOLEAN }
        }),
        std::make_tuple<std::string, size_t, std::vector<column_def>>("dates_and_times.csv", 5, {
            { .name = "date", .type = duckdb::LogicalType::DATE },
            { .name = "time", .type = duckdb::LogicalType::TIME }, // TODO: Do we want this column? Fivetran doesn't support TIME type
            { .name = "datetime", .type = duckdb::LogicalType::TIMESTAMP },
            { .name = "timestamp", .type = duckdb::LogicalType::TIMESTAMP_TZ }
        }),
        std::make_tuple<std::string, size_t, std::vector<column_def>>("large.csv", 1000, {
            { .name = "transaction_id", .type = duckdb::LogicalType::INTEGER },
            { .name = "user_id", .type = duckdb::LogicalType::INTEGER },
            { .name = "product_name", .type = duckdb::LogicalType::VARCHAR },
            { .name = "category", .type = duckdb::LogicalType::VARCHAR },
            { .name = "amount", .type = duckdb::LogicalType::FLOAT },
            { .name = "quantity", .type = duckdb::LogicalType::INTEGER },
            { .name = "discount", .type = duckdb::LogicalType::DOUBLE },
            { .name = "tax", .type = duckdb::LogicalType::DOUBLE },
            { .name = "shipping_cost", .type = duckdb::LogicalType::DOUBLE },
            { .name = "payment_method", .type = duckdb::LogicalType::VARCHAR },
            { .name = "status", .type = duckdb::LogicalType::VARCHAR },
            { .name = "order_date", .type = duckdb::LogicalType::DATE }
        }),
        std::make_tuple<std::string, size_t, std::vector<column_def>>("many_columns.csv", 3, {
            { .name = "col1", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col2", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col3", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col4", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col5", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col6", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col7", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col8", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col9", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col10", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col11", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col12", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col13", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col14", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col15", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col16", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col17", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col18", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col19", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col20", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col21", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col22", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col23", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col24", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col25", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col26", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col27", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col28", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col29", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col30", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col31", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col32", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col33", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col34", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col35", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col36", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col37", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col38", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col39", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col40", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col41", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col42", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col43", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col44", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col45", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col46", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col47", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col48", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col49", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col50", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col51", .type = duckdb::LogicalType::VARCHAR },
            { .name = "col52", .type = duckdb::LogicalType::VARCHAR }
        })
    }));

  // const auto filename =
  //     GENERATE("booleans.csv", "dates_and_times.csv", "large.csv",
  //              "many_columns.csv", "mixed_types.csv", "nulls_and_empty.csv",
  //              "numeric_types.csv", "single_column.csv", "special_chars.csv");
  const fs::path test_file = fs::path(TEST_RESOURCES_DIR) / "csv" / filename;
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  IngestProperties props(test_file.string(), "", columns, "", 1);
  auto logger = std::make_shared<mdlog::MdLog>();
  csv_processor::ProcessFile(
      con, props, logger, [&con, expected_row_count = row_count](const std::string &view_name) {
        const auto read_csv_result = con.Query("FROM " + view_name);
        if (read_csv_result->HasError()) {
          FAIL("Failed to execute comparison query: " +
               read_csv_result->GetError());
        }
        CHECK(read_csv_result->RowCount() == expected_row_count);
      });
}

TEST_CASE("Test reading zstd-compressed CSV files", "[csv_processor]") {
  const auto filename =
      GENERATE("customers-100000.csv.zst", "train_few_shot.csv.zst");
  const fs::path test_file =
      fs::path(TEST_RESOURCES_DIR) / "compressed_csv" / filename;
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  IngestProperties props(test_file.string(), "", {}, "", 1);
  auto logger = std::make_shared<mdlog::MdLog>();
  csv_processor::ProcessFile(
      con, props, logger, [&con, &test_file](const std::string &view_name) {
        // Find the difference between the view and the original CSV data;
        // should be empty
        const std::string compare_query =
            "WITH expected_data AS (FROM read_csv('" + test_file.string() +
            "', header=true, encoding='utf-8', compression='zstd')) "
            "(FROM " +
            view_name +
            " EXCEPT FROM expected_data) "
            "UNION ALL "
            "(FROM expected_data EXCEPT FROM " +
            view_name + ")";
        const auto compare_result = con.Query(compare_query);
        if (compare_result->HasError()) {
          FAIL("Failed to execute 2 query: " + compare_result->GetError());
        }
        CHECK(compare_result->RowCount() == 0);
      });
}

TEST_CASE("Test reading files generated by Fivetran destination tester", "[csv_processor]") {
    // TODO: Make mixed_data_input_4_upsert.csv.zstd pass. The solution is to specify a schema when reading the CSV
    // because Arrow and DuckDB infer different types for the 'binary_val' column.
    const auto filename =
        GENERATE("campaign_input_1_delete.csv.zstd", "campaign_input_1_update.csv.zstd", "campaign_input_1_upsert.csv.zstd",
            "customers_input_2_delete.csv.zstd", "customers_input_2_update.csv.zstd", "customers_input_2_upsert.csv.zstd", /* "mixed_data_input_4_upsert.csv.zstd", */
            "orders_input_2_update.csv.zstd", "orders_input_2_upsert.csv.zstd", "products_input_2_upsert.csv.zstd", "transaction_input_1_delete.csv.zstd",
            "transaction_input_1_update.csv.zstd", "transaction_input_1_upsert.csv.zstd", "user_profiles_input_3_upsert.csv.zstd", "web_events_input_3_upsert.csv.zstd");
    const fs::path test_file =
        fs::path(TEST_RESOURCES_DIR) / "destination_tester" / "generated_files" / filename;
    CAPTURE(test_file);
    REQUIRE(fs::exists(test_file));

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    IngestProperties props(test_file.string(), "", {}, "", 1);
    auto logger = std::make_shared<mdlog::MdLog>();
    csv_processor::ProcessFile(
        con, props, logger, [&con, &test_file](const std::string &view_name) {
          // Find the difference between the view and the original CSV data;
          // should be empty
          const std::string compare_query =
              "WITH expected_data AS (FROM read_csv('" + test_file.string() +
              "', header=true, encoding='utf-8', compression='zstd')) "
              "(FROM " +
              view_name +
              " EXCEPT FROM expected_data) "
              "UNION ALL "
              "(FROM expected_data EXCEPT FROM " +
              view_name + ")";
          const auto compare_result = con.Query(compare_query);
          if (compare_result->HasError()) {
            FAIL("Failed to execute 2 query: " + compare_result->GetError());
          }
          CHECK(compare_result->RowCount() == 0);
        });
}
