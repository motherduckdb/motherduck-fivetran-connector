#include "constants.hpp"
#include "csv_processor.hpp"
#include "duckdb.hpp"
#include "schema_types.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <filesystem>
#include <fstream>
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

  std::vector<column_def> columns{
      column_def{.name = "id", .type = duckdb::LogicalType::INTEGER},
      column_def{.name = "name", .type = duckdb::LogicalType::VARCHAR},
      column_def{.name = "age", .type = duckdb::LogicalType::SMALLINT}};
  IngestProperties props(test_file.string(), "", columns, "", 1);
  auto logger = std::make_shared<mdlog::MdLog>();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &view_name) {
        const auto res = con.Query("FROM " + view_name);
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

TEST_CASE("Test reading CSV file with auto-detection of column types",
          "[csv_processor]") {
  const fs::path test_file =
      fs::path(TEST_RESOURCES_DIR) / "csv" / "small_simple.csv";
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  std::vector<column_def> columns{
      column_def{.name = "id", .type = duckdb::LogicalType::INVALID},
      column_def{.name = "name", .type = duckdb::LogicalType::INVALID},
      column_def{.name = "age", .type = duckdb::LogicalType::INVALID}};
  IngestProperties props(test_file.string(), "", columns, "", 1);
  auto logger = std::make_shared<mdlog::MdLog>();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &view_name) {
        const auto res = con.Query("FROM " + view_name);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->ColumnCount() == 3);
        CHECK(res->RowCount() == 3);

        // CSV reader uses BIGINT for integer columns when auto-detecting.
        // This would get converted to the corresponding column type of a table
        // during insertion.
        CHECK(res->types[0].id() == duckdb::LogicalTypeId::BIGINT);
        CHECK(res->types[1].id() == duckdb::LogicalTypeId::VARCHAR);
        CHECK(res->types[2].id() == duckdb::LogicalTypeId::BIGINT);
      });
}

TEST_CASE("Test reading CSV file with columns out of order",
          "[csv_processor]") {
  const fs::path test_file =
      fs::path(TEST_RESOURCES_DIR) / "csv" / "small_simple.csv";
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  // In the CSV file, the order is id, name, age
  std::vector<column_def> columns{
      column_def{.name = "name", .type = duckdb::LogicalType::VARCHAR},
      column_def{.name = "age", .type = duckdb::LogicalType::SMALLINT},
      column_def{.name = "id", .type = duckdb::LogicalType::INTEGER},
  };
  IngestProperties props(test_file.string(), "", columns, "", 1);
  auto logger = std::make_shared<mdlog::MdLog>();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &view_name) {
        const std::string query = "FROM " + view_name + " ORDER BY id LIMIT 1";
        auto res = con.Query(query);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->ColumnCount() == 3);
        CHECK(res->RowCount() == 1);

        REQUIRE(res->GetValue(0, 0).ToString() == "Alice");
        REQUIRE(res->GetValue(1, 0).GetValue<int>() == 30);
        REQUIRE(res->GetValue(2, 0).GetValue<int>() == 1);
      });
}

// TODO: Test cases:
// - Test with different column data types (e.g. different int types). Also edge
// cases like overflow
// - various error paths
// - CSV block size
// - Pass encryption key when file not encrypted
// - Different encoding
// - Different newline
// - Different delimiter??

TEST_CASE("Test reading various CSV files", "[csv_processor]") {
  auto [filename, row_count, columns] = GENERATE(table<std::string, size_t,
                                                       std::vector<column_def>>(
      {std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "booleans.csv", 6,
           {{.name = "id", .type = duckdb::LogicalTypeId::INTEGER},
            {.name = "bool_true_false",
             .type =
                 duckdb::LogicalTypeId::BOOLEAN}}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "dates_and_times.csv", 5,
           {{.name = "date", .type = duckdb::LogicalTypeId::DATE},
            {.name = "time",
             .type =
                 duckdb::LogicalTypeId::TIME}, // TODO: Do we want this column?
                                               // Fivetran doesn't support TIME
                                               // type
            {.name = "datetime", .type = duckdb::LogicalTypeId::TIMESTAMP},
            {.name = "timestamp",
             .type =
                 duckdb::LogicalTypeId::TIMESTAMP_TZ}}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "large.csv", 1000,
           {{.name = "transaction_id", .type = duckdb::LogicalTypeId::INTEGER},
            {.name = "user_id", .type = duckdb::LogicalTypeId::INTEGER},
            {.name = "product_name", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "category", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "amount", .type = duckdb::LogicalTypeId::FLOAT},
            {.name = "quantity", .type = duckdb::LogicalTypeId::INTEGER},
            {.name = "discount", .type = duckdb::LogicalTypeId::DOUBLE},
            {.name = "tax", .type = duckdb::LogicalTypeId::DOUBLE},
            {.name = "shipping_cost", .type = duckdb::LogicalTypeId::DOUBLE},
            {.name = "payment_method", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "status", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "order_date", .type = duckdb::LogicalTypeId::DATE}}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "many_columns.csv", 3,
           {{.name = "col1", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col2", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col3", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col4", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col5", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col6", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col7", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col8", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col9", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col10", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col11", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col12", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col13", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col14", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col15", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col16", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col17", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col18", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col19", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col20", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col21", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col22", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col23", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col24", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col25", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col26", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col27", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col28", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col29", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col30", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col31", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col32", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col33", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col34", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col35", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col36", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col37", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col38", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col39", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col40", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col41", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col42", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col43", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col44", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col45", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col46", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col47", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col48", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col49", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col50", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col51", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "col52", .type = duckdb::LogicalTypeId::VARCHAR}}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "mixed_types.csv", 5,
           {{.name = "id", .type = duckdb::LogicalTypeId::INTEGER},
            {.name = "string_col", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "int_col", .type = duckdb::LogicalTypeId::INTEGER},
            {.name = "float_col", .type = duckdb::LogicalTypeId::FLOAT},
            {.name = "bool_col", .type = duckdb::LogicalTypeId::BOOLEAN},
            {.name = "date_col", .type = duckdb::LogicalTypeId::DATE},
            {.name = "mixed_col", .type = duckdb::LogicalTypeId::VARCHAR}}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "nulls_and_empty.csv", 6,
           {{.name = "id", .type = duckdb::LogicalTypeId::INTEGER},
            {.name = "nullable_string", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "nullable_int", .type = duckdb::LogicalTypeId::INTEGER},
            {.name = "nullable_float", .type = duckdb::LogicalTypeId::FLOAT}}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "numeric_types.csv", 5,
           {{.name = "smallint_col", .type = duckdb::LogicalTypeId::SMALLINT},
            {.name = "int_col", .type = duckdb::LogicalTypeId::INTEGER},
            {.name = "bigint_col", .type = duckdb::LogicalTypeId::BIGINT},
            {.name = "float_col", .type = duckdb::LogicalTypeId::FLOAT},
            {.name = "double_col", .type = duckdb::LogicalTypeId::DOUBLE},
            {.name = "decimal_5_3_col",
             .type = duckdb::LogicalTypeId::DECIMAL,
             .width = 5,
             .scale = 3}}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "single_column.csv", 10,
           {{.name = "value", .type = duckdb::LogicalTypeId::SMALLINT}}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "special_chars.csv", 6,
           {{.name = "id", .type = duckdb::LogicalTypeId::INTEGER},
            {.name = "text", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "description",
             .type = duckdb::LogicalTypeId::VARCHAR}})}));

  const fs::path test_file = fs::path(TEST_RESOURCES_DIR) / "csv" / filename;
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  IngestProperties props(test_file.string(), "", columns, "", 1);
  auto logger = std::make_shared<mdlog::MdLog>();
  csv_processor::ProcessFile(
      con, props, logger,
      [&con, expected_row_count = row_count](const std::string &view_name) {
        const auto res = con.Query("FROM " + view_name);
        if (res->HasError()) {
          FAIL("Failed to execute comparison query: " + res->GetError());
        }
        CHECK(res->RowCount() == expected_row_count);
      });
}

TEST_CASE("Test reading CSV file with special null string", "[csv_processor]") {
  const fs::path test_file =
      fs::path(TEST_RESOURCES_DIR) / "csv" / "special_null.csv";
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  const std::vector<column_def> columns{
      column_def{.name = "id", .type = duckdb::LogicalType::INTEGER},
      column_def{.name = "name", .type = duckdb::LogicalType::VARCHAR},
      column_def{.name = "age", .type = duckdb::LogicalType::SMALLINT},
      column_def{.name = "country", .type = duckdb::LogicalType::VARCHAR}};
  const std::string null_string = "special-null";
  IngestProperties props(test_file.string(), "", columns, null_string, 1);
  auto logger = std::make_shared<mdlog::MdLog>();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &view_name) {
        const auto res = con.Query("FROM " + view_name);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->ColumnCount() == 4);
        REQUIRE(res->RowCount() == 2);

        for (idx_t row = 0; row < res->RowCount(); row++) {
          if (row == 0) {
            REQUIRE(1 == res->GetValue(0, row).GetValue<int>());
            REQUIRE("Alice" == res->GetValue(1, row).ToString());
            REQUIRE(res->GetValue(2, row).IsNull());
            REQUIRE("The Netherlands" == res->GetValue(3, row).ToString());
          } else if (row == 1) {
            REQUIRE(2 == res->GetValue(0, row).GetValue<int>());
            REQUIRE("Bob" == res->GetValue(1, row).ToString());
            REQUIRE(30 == res->GetValue(2, row).GetValue<int>());
            REQUIRE(res->GetValue(3, row).IsNull());
          }
        }
      });
}

TEST_CASE("Test reading CSV file with unmodified string setting",
          "[csv_processor]") {
  const fs::path test_file =
      fs::path(TEST_RESOURCES_DIR) / "csv" / "unmodified_string.csv";
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  const std::vector<column_def> columns{
      column_def{.name = "id", .type = duckdb::LogicalType::INTEGER},
      column_def{.name = "name", .type = duckdb::LogicalType::VARCHAR},
      column_def{.name = "age", .type = duckdb::LogicalType::SMALLINT}};

  IngestProperties props(test_file.string(), "", columns, "", 1);
  props.allow_unmodified_string = true;
  auto logger = std::make_shared<mdlog::MdLog>();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &view_name) {
        const auto res = con.Query("FROM " + view_name);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->ColumnCount() == 3);
        REQUIRE(res->RowCount() == 2);

        CHECK(res->types[0] == duckdb::LogicalTypeId::VARCHAR);
        CHECK(res->types[1] == duckdb::LogicalTypeId::VARCHAR);
        CHECK(res->types[2] == duckdb::LogicalTypeId::VARCHAR);

        for (idx_t row = 0; row < res->RowCount(); row++) {
          if (row == 0) {
            REQUIRE("1" == res->GetValue(0, row).ToString());
            REQUIRE("unmodified_string" == res->GetValue(1, row).ToString());
            REQUIRE("30" == res->GetValue(2, row).ToString());
          } else if (row == 1) {
            REQUIRE("2" == res->GetValue(0, row).ToString());
            REQUIRE("Bob" == res->GetValue(1, row).ToString());
            REQUIRE("unmodified_string" == res->GetValue(2, row).ToString());
          }
        }
      });
}

TEST_CASE("Test reading zstd-compressed CSV files", "[csv_processor]") {
  auto [filename, row_count, columns] = GENERATE(table<std::string, size_t,
                                                       std::vector<column_def>>(
      {std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "customers-100000.csv.zst", 100000,
           {{.name = "Index", .type = duckdb::LogicalTypeId::INTEGER},
            {.name = "Customer Id", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "First Name", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "Last Name", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "Company", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "City", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "Country", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "Phone 1", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "Phone 2", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "Email", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "Subscription Date", .type = duckdb::LogicalTypeId::DATE},
            {.name = "Website", .type = duckdb::LogicalTypeId::VARCHAR}}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "train_few_shot.csv.zst", 40000,
           {{.name = "text", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "label", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "source", .type = duckdb::LogicalTypeId::VARCHAR},
            {.name = "reasoning", .type = duckdb::LogicalTypeId::VARCHAR}})}));

  const fs::path test_file =
      fs::path(TEST_RESOURCES_DIR) / "compressed_csv" / filename;
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  IngestProperties props(test_file.string(), "", columns, "", 1);
  auto logger = std::make_shared<mdlog::MdLog>();
  csv_processor::ProcessFile(
      con, props, logger,
      [&con, expected_row_count = row_count](const std::string &view_name) {
        const auto res = con.Query("FROM " + view_name);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->RowCount() == expected_row_count);
      });
}

TEST_CASE("Test reading files generated by Fivetran destination tester",
          "[csv_processor]") {

  // Test reading all different files: uncompressed, zstd-compressed, and
  // zstd+AES
  const auto file_extension = GENERATE(".csv", ".csv.zstd", ".csv.zstd.aes");

  auto [filename, row_count, null_string, can_contain_unmodified,
        columns] = GENERATE(table<std::string, size_t, std::string, bool,
                                  std::vector<column_def>>({
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "campaign_input_1_upsert", 3, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          false,
          {{.name = "name", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "num",
            .type = duckdb::LogicalTypeId::DECIMAL,
            .width = 6,
            .scale = 3},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "_fivetran_id",
            .type = duckdb::LogicalTypeId::VARCHAR,
            .primary_key = true},
           {.name = "_fivetran_deleted",
            .type = duckdb::LogicalTypeId::BOOLEAN}}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "campaign_input_1_update", 1, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          true,
          {{.name = "name", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "num",
            .type = duckdb::LogicalTypeId::DECIMAL,
            .width = 6,
            .scale = 3},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "_fivetran_id",
            .type = duckdb::LogicalTypeId::VARCHAR,
            .primary_key = true},
           {.name = "_fivetran_deleted",
            .type = duckdb::LogicalTypeId::BOOLEAN}}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "campaign_input_1_delete", 1, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          false,
          {{.name = "name", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "num",
            .type = duckdb::LogicalTypeId::DECIMAL,
            .width = 6,
            .scale = 3},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "_fivetran_id",
            .type = duckdb::LogicalTypeId::VARCHAR,
            .primary_key = true},
           {.name = "_fivetran_deleted",
            .type = duckdb::LogicalTypeId::BOOLEAN}}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "transaction_input_1_upsert", 7,
          "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP", false,
          {{.name = "id",
            .type = duckdb::LogicalTypeId::INTEGER,
            .primary_key = true},
           {.name = "amount", .type = duckdb::LogicalTypeId::FLOAT},
           {.name = "desc", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "_fivetran_deleted",
            .type = duckdb::LogicalTypeId::BOOLEAN}}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "transaction_input_1_update", 4,
          "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP", true,
          {{.name = "id",
            .type = duckdb::LogicalTypeId::INTEGER,
            .primary_key = true},
           {.name = "amount", .type = duckdb::LogicalTypeId::FLOAT},
           {.name = "desc", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "_fivetran_deleted",
            .type = duckdb::LogicalTypeId::BOOLEAN}}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "transaction_input_1_delete", 2,
          "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP", false,
          {{.name = "id",
            .type = duckdb::LogicalTypeId::INTEGER,
            .primary_key = true},
           {.name = "amount", .type = duckdb::LogicalTypeId::FLOAT},
           {.name = "desc", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "_fivetran_deleted",
            .type = duckdb::LogicalTypeId::BOOLEAN}}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "orders_input_2_upsert", 3, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          false,
          {{.name = "order_id",
            .type = duckdb::LogicalTypeId::VARCHAR,
            .primary_key = true},
           {.name = "customer_id", .type = duckdb::LogicalTypeId::BIGINT},
           {.name = "order_date", .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "status", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "total_amount", .type = duckdb::LogicalTypeId::FLOAT},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "_fivetran_deleted",
            .type = duckdb::LogicalTypeId::BOOLEAN}}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "orders_input_2_update", 3, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          true,
          {{.name = "order_id",
            .type = duckdb::LogicalTypeId::VARCHAR,
            .primary_key = true},
           {.name = "customer_id", .type = duckdb::LogicalTypeId::BIGINT},
           {.name = "order_date", .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "status", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "total_amount", .type = duckdb::LogicalTypeId::FLOAT},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "_fivetran_deleted",
            .type = duckdb::LogicalTypeId::BOOLEAN}}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "customers_input_2_upsert", 6, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          false,
          {{.name = "customer_id",
            .type = duckdb::LogicalTypeId::BIGINT,
            .primary_key = true},
           {.name = "first_name", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "last_name", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "email", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "registration_date", .type = duckdb::LogicalTypeId::DATE},
           {.name = "total_spent",
            .type = duckdb::LogicalTypeId::DECIMAL,
            .width = 12,
            .scale = 2},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "loyalty_tier", .type = duckdb::LogicalTypeId::VARCHAR}}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "customers_input_2_update", 2, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          true,
          {{.name = "customer_id",
            .type = duckdb::LogicalTypeId::BIGINT,
            .primary_key = true},
           {.name = "first_name", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "last_name", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "email", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "registration_date", .type = duckdb::LogicalTypeId::DATE},
           {.name = "total_spent",
            .type = duckdb::LogicalTypeId::DECIMAL,
            .width = 12,
            .scale = 2},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "loyalty_tier", .type = duckdb::LogicalTypeId::VARCHAR}}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "customers_input_2_delete", 1, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          false,
          {{.name = "customer_id",
            .type = duckdb::LogicalTypeId::BIGINT,
            .primary_key = true},
           {.name = "first_name", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "last_name", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "email", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "registration_date", .type = duckdb::LogicalTypeId::DATE},
           {.name = "total_spent",
            .type = duckdb::LogicalTypeId::DECIMAL,
            .width = 12,
            .scale = 2},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "loyalty_tier", .type = duckdb::LogicalTypeId::VARCHAR}}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "products_input_2_upsert", 5, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          false,
          {{.name = "product_name", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "category", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "unit_price", .type = duckdb::LogicalTypeId::DOUBLE},
           {.name = "stock_count", .type = duckdb::LogicalTypeId::INTEGER},
           {.name = "is_active", .type = duckdb::LogicalTypeId::BOOLEAN},
           {.name = "specs_json", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "manual_xml", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "_fivetran_id",
            .type = duckdb::LogicalTypeId::VARCHAR,
            .primary_key = true}}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "user_profiles_input_3_upsert", 24,
          "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP", false,
          {{.name = "user_id",
            .type = duckdb::LogicalTypeId::BIGINT,
            .primary_key = true},
           {.name = "username", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "email", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "created_at", .type = duckdb::LogicalTypeId::TIMESTAMP},
           {.name = "is_active", .type = duckdb::LogicalTypeId::BOOLEAN},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ}}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "web_events_input_3_upsert", 154,
          "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP", false,
          {{.name = "event_id",
            .type = duckdb::LogicalTypeId::VARCHAR,
            .primary_key = true},
           {.name = "session_id", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "user_id", .type = duckdb::LogicalTypeId::BIGINT},
           {.name = "event_timestamp",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "event_type", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "url", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "ip_address", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "user_agent", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "payload", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "processing_time_ms",
            .type = duckdb::LogicalTypeId::INTEGER},
           {.name = "is_error", .type = duckdb::LogicalTypeId::BOOLEAN},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ}}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "mixed_data_input_4_upsert", 1,
          "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP", false,
          {{.name = "bool_val", .type = duckdb::LogicalTypeId::BOOLEAN},
           {.name = "short_val", .type = duckdb::LogicalTypeId::SMALLINT},
           {.name = "int_val", .type = duckdb::LogicalTypeId::INTEGER},
           {.name = "long_val", .type = duckdb::LogicalTypeId::BIGINT},
           {.name = "float_val", .type = duckdb::LogicalTypeId::FLOAT},
           {.name = "double_val", .type = duckdb::LogicalTypeId::DOUBLE},
           {.name = "decimal_val",
            .type = duckdb::LogicalTypeId::DECIMAL,
            .width = 38,
            .scale = 10},
           {.name = "naive_date_val", .type = duckdb::LogicalTypeId::DATE},
           {.name = "naive_datetime_val",
            .type = duckdb::LogicalTypeId::TIMESTAMP},
           {.name = "utc_datetime_val",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
           {.name = "binary_val", .type = duckdb::LogicalTypeId::BIT},
           {.name = "json_val", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "string_val", .type = duckdb::LogicalTypeId::VARCHAR},
           {.name = "_fivetran_synced",
            .type = duckdb::LogicalTypeId::TIMESTAMP_TZ}}),
  }));

  const fs::path test_file = fs::path(TEST_RESOURCES_DIR) /
                             "destination_tester" / "generated_files" /
                             (filename + file_extension);
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  std::string decryption_key;
  if (duckdb::StringUtil::EndsWith(file_extension, ".aes")) {
    const auto key_file = test_file.string() + ".key";
    REQUIRE(fs::exists(key_file));
    std::ifstream key_stream(key_file, std::ios::binary);
    // Read the entire key file
    decryption_key.assign(std::istreambuf_iterator<char>(key_stream),
                          std::istreambuf_iterator<char>());
  }

  IngestProperties props(test_file.string(), decryption_key, columns,
                         null_string, 1);
  props.allow_unmodified_string = can_contain_unmodified;

  auto logger = std::make_shared<mdlog::MdLog>();
  csv_processor::ProcessFile(
      con, props, logger,
      [&con, expected_row_count = row_count](const std::string &view_name) {
        const auto res = con.Query("FROM " + view_name);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->RowCount() == expected_row_count);
      });
}
