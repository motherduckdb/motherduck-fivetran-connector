#include "constants.hpp"
#include "csv_processor.hpp"
#include "duckdb.hpp"
#include "schema_types.hpp"
#include "test_helpers.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <filesystem>
#include <fstream>
#include <string>
#include <tuple>
#include <vector>

#include "catch2/matchers/catch_matchers_string.hpp"

namespace fs = std::filesystem;
using namespace test::constants;
using namespace test_helpers;

TEST_CASE("Test can read simple CSV file", "[csv_processor]") {
  const fs::path test_file =
      fs::path(TEST_RESOURCES_DIR) / "csv" / "small_simple.csv";
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  std::vector<column_def> columns{
      col("id", duckdb::LogicalTypeId::INTEGER),
      col("name", duckdb::LogicalTypeId::VARCHAR),
      col("age", duckdb::LogicalTypeId::SMALLINT)};
  auto props = make_props(test_file.string(), columns);
  auto logger = mdlog::Logger::CreateNopLogger();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &staging_table_name) {
        const auto res = con.Query("FROM " + staging_table_name);
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
      col("id", duckdb::LogicalTypeId::INVALID),
      col("name", duckdb::LogicalTypeId::INVALID),
      col("age", duckdb::LogicalTypeId::INVALID)};
  auto props = make_props(test_file.string(), columns);
  auto logger = mdlog::Logger::CreateNopLogger();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &staging_table_name) {
        const auto res = con.Query("FROM " + staging_table_name);
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

TEST_CASE("Test reading CSV file when not all column types specified",
          "[csv_processor]") {
  const fs::path test_file =
      fs::path(TEST_RESOURCES_DIR) / "csv" / "small_simple.csv";
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  // In the CSV file, the order is id, name, age
  std::vector<column_def> columns{
      col("id", duckdb::LogicalTypeId::INTEGER),
      col("name", duckdb::LogicalTypeId::VARCHAR),
      // Column "age" is not added to column_types
      col("age", duckdb::LogicalTypeId::INVALID),
  };
  auto props = make_props(test_file.string(), columns);
  auto logger = mdlog::Logger::CreateNopLogger();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &staging_table_name) {
        const auto res = con.Query("FROM " + staging_table_name);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->ColumnCount() == 3);
        CHECK(res->RowCount() == 3);

        CHECK(res->types[0].id() == duckdb::LogicalTypeId::INTEGER);
        CHECK(res->types[1].id() == duckdb::LogicalTypeId::VARCHAR);
        // Auto-detected type for "age" column
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
      col("name", duckdb::LogicalTypeId::VARCHAR),
      col("age", duckdb::LogicalTypeId::SMALLINT),
      col("id", duckdb::LogicalTypeId::INTEGER),
  };
  auto props = make_props(test_file.string(), columns);
  auto logger = mdlog::Logger::CreateNopLogger();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &staging_table_name) {
        const std::string query =
            "FROM " + staging_table_name + " ORDER BY id LIMIT 1";
        auto res = con.Query(query);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->ColumnCount() == 3);
        CHECK(res->RowCount() == 1);

        REQUIRE(res->GetValue(0, 0).ToString() == "Alice");
        REQUIRE(res->GetValue(1, 0).GetValue<int>() == 30);
        REQUIRE(res->GetValue(2, 0).GetValue<int>() == 1);
      });
}

TEST_CASE("Test reading CSV file with quotes in filename", "[csv_processor]") {
  const fs::path test_file =
      fs::path(TEST_RESOURCES_DIR) / "csv" / "filename_with_'quotes'.csv";
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  std::vector<column_def> columns{col("a", duckdb::LogicalTypeId::SMALLINT)};
  auto props = make_props(test_file.string(), columns);
  auto logger = mdlog::Logger::CreateNopLogger();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &staging_table_name) {
        const auto res = con.Query("FROM " + staging_table_name);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->ColumnCount() == 1);
        CHECK(res->RowCount() == 1);
      });
}

TEST_CASE("Test reading various CSV files", "[csv_processor]") {
  auto [filename, row_count, columns] = GENERATE(table<std::string, size_t,
                                                       std::vector<column_def>>(
      {std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "booleans.csv", 6,
           {col("id", duckdb::LogicalTypeId::INTEGER),
            col("bool_true_false", duckdb::LogicalTypeId::BOOLEAN)}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "dates_and_times.csv", 5,
           {col("date", duckdb::LogicalTypeId::DATE),
            col("time", duckdb::LogicalTypeId::TIME),
            col("datetime", duckdb::LogicalTypeId::TIMESTAMP),
            col("timestamp", duckdb::LogicalTypeId::TIMESTAMP_TZ)}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "large.csv", 1000,
           {col("transaction_id", duckdb::LogicalTypeId::INTEGER),
            col("user_id", duckdb::LogicalTypeId::INTEGER),
            col("product_name", duckdb::LogicalTypeId::VARCHAR),
            col("category", duckdb::LogicalTypeId::VARCHAR),
            col("amount", duckdb::LogicalTypeId::FLOAT),
            col("quantity", duckdb::LogicalTypeId::INTEGER),
            col("discount", duckdb::LogicalTypeId::DOUBLE),
            col("tax", duckdb::LogicalTypeId::DOUBLE),
            col("shipping_cost", duckdb::LogicalTypeId::DOUBLE),
            col("payment_method", duckdb::LogicalTypeId::VARCHAR),
            col("status", duckdb::LogicalTypeId::VARCHAR),
            col("order_date", duckdb::LogicalTypeId::DATE)}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "many_columns.csv", 3,
           {col("col1", duckdb::LogicalTypeId::VARCHAR),
            col("col2", duckdb::LogicalTypeId::VARCHAR),
            col("col3", duckdb::LogicalTypeId::VARCHAR),
            col("col4", duckdb::LogicalTypeId::VARCHAR),
            col("col5", duckdb::LogicalTypeId::VARCHAR),
            col("col6", duckdb::LogicalTypeId::VARCHAR),
            col("col7", duckdb::LogicalTypeId::VARCHAR),
            col("col8", duckdb::LogicalTypeId::VARCHAR),
            col("col9", duckdb::LogicalTypeId::VARCHAR),
            col("col10", duckdb::LogicalTypeId::VARCHAR),
            col("col11", duckdb::LogicalTypeId::VARCHAR),
            col("col12", duckdb::LogicalTypeId::VARCHAR),
            col("col13", duckdb::LogicalTypeId::VARCHAR),
            col("col14", duckdb::LogicalTypeId::VARCHAR),
            col("col15", duckdb::LogicalTypeId::VARCHAR),
            col("col16", duckdb::LogicalTypeId::VARCHAR),
            col("col17", duckdb::LogicalTypeId::VARCHAR),
            col("col18", duckdb::LogicalTypeId::VARCHAR),
            col("col19", duckdb::LogicalTypeId::VARCHAR),
            col("col20", duckdb::LogicalTypeId::VARCHAR),
            col("col21", duckdb::LogicalTypeId::VARCHAR),
            col("col22", duckdb::LogicalTypeId::VARCHAR),
            col("col23", duckdb::LogicalTypeId::VARCHAR),
            col("col24", duckdb::LogicalTypeId::VARCHAR),
            col("col25", duckdb::LogicalTypeId::VARCHAR),
            col("col26", duckdb::LogicalTypeId::VARCHAR),
            col("col27", duckdb::LogicalTypeId::VARCHAR),
            col("col28", duckdb::LogicalTypeId::VARCHAR),
            col("col29", duckdb::LogicalTypeId::VARCHAR),
            col("col30", duckdb::LogicalTypeId::VARCHAR),
            col("col31", duckdb::LogicalTypeId::VARCHAR),
            col("col32", duckdb::LogicalTypeId::VARCHAR),
            col("col33", duckdb::LogicalTypeId::VARCHAR),
            col("col34", duckdb::LogicalTypeId::VARCHAR),
            col("col35", duckdb::LogicalTypeId::VARCHAR),
            col("col36", duckdb::LogicalTypeId::VARCHAR),
            col("col37", duckdb::LogicalTypeId::VARCHAR),
            col("col38", duckdb::LogicalTypeId::VARCHAR),
            col("col39", duckdb::LogicalTypeId::VARCHAR),
            col("col40", duckdb::LogicalTypeId::VARCHAR),
            col("col41", duckdb::LogicalTypeId::VARCHAR),
            col("col42", duckdb::LogicalTypeId::VARCHAR),
            col("col43", duckdb::LogicalTypeId::VARCHAR),
            col("col44", duckdb::LogicalTypeId::VARCHAR),
            col("col45", duckdb::LogicalTypeId::VARCHAR),
            col("col46", duckdb::LogicalTypeId::VARCHAR),
            col("col47", duckdb::LogicalTypeId::VARCHAR),
            col("col48", duckdb::LogicalTypeId::VARCHAR),
            col("col49", duckdb::LogicalTypeId::VARCHAR),
            col("col50", duckdb::LogicalTypeId::VARCHAR),
            col("col51", duckdb::LogicalTypeId::VARCHAR),
            col("col52", duckdb::LogicalTypeId::VARCHAR)}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "mixed_types.csv", 5,
           {col("id", duckdb::LogicalTypeId::INTEGER),
            col("string_col", duckdb::LogicalTypeId::VARCHAR),
            col("int_col", duckdb::LogicalTypeId::INTEGER),
            col("float_col", duckdb::LogicalTypeId::FLOAT),
            col("bool_col", duckdb::LogicalTypeId::BOOLEAN),
            col("date_col", duckdb::LogicalTypeId::DATE),
            col("mixed_col", duckdb::LogicalTypeId::VARCHAR)}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "nulls_and_empty.csv", 6,
           {col("id", duckdb::LogicalTypeId::INTEGER),
            col("nullable_string", duckdb::LogicalTypeId::VARCHAR),
            col("nullable_int", duckdb::LogicalTypeId::INTEGER),
            col("nullable_float", duckdb::LogicalTypeId::FLOAT)}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "numeric_types.csv", 5,
           {col("smallint_col", duckdb::LogicalTypeId::SMALLINT),
            col("int_col", duckdb::LogicalTypeId::INTEGER),
            col("bigint_col", duckdb::LogicalTypeId::BIGINT),
            col("float_col", duckdb::LogicalTypeId::FLOAT),
            col("double_col", duckdb::LogicalTypeId::DOUBLE),
            decimal_col("decimal_5_3_col", 5, 3)}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "single_column.csv", 10,
           {col("value", duckdb::LogicalTypeId::SMALLINT)}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "special_chars.csv", 6,
           {col("id", duckdb::LogicalTypeId::INTEGER),
            col("text", duckdb::LogicalTypeId::VARCHAR),
            col("description", duckdb::LogicalTypeId::VARCHAR)})}));

  const fs::path test_file = fs::path(TEST_RESOURCES_DIR) / "csv" / filename;
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  auto props = make_props(test_file.string(), columns);
  auto logger = mdlog::Logger::CreateNopLogger();
  csv_processor::ProcessFile(
      con, props, logger,
      [&con,
       expected_row_count = row_count](const std::string &staging_table_name) {
        const auto res = con.Query("FROM " + staging_table_name);
        if (res->HasError()) {
          FAIL("Failed to execute \"FROM csv_view\" query: " + res->GetError());
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
      col("id", duckdb::LogicalTypeId::INTEGER),
      col("name", duckdb::LogicalTypeId::VARCHAR),
      col("age", duckdb::LogicalTypeId::SMALLINT),
      col("country", duckdb::LogicalTypeId::VARCHAR)};
  const std::string null_string = "special-null";
  auto props = make_props(test_file.string(), columns, "", null_string);
  auto logger = mdlog::Logger::CreateNopLogger();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &staging_table_name) {
        const auto res = con.Query("FROM " + staging_table_name);
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

TEST_CASE("Test reading CSV file with escaped string", "[csv_processor]") {
  const fs::path test_file =
      fs::path(TEST_RESOURCES_DIR) / "csv" / "escaped_string.csv";
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  const std::vector<column_def> columns{
      col("escaped_string", duckdb::LogicalTypeId::VARCHAR)};
  auto props = make_props(test_file.string(), columns);
  auto logger = mdlog::Logger::CreateNopLogger();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &staging_table_name) {
        const auto res = con.Query("FROM " + staging_table_name);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->ColumnCount() == 1);
        REQUIRE(res->RowCount() == 1);

        REQUIRE("t\"\"es\"t\"1" == res->GetValue(0, 0).ToString());
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
      col("id", duckdb::LogicalTypeId::INTEGER),
      col("name", duckdb::LogicalTypeId::VARCHAR),
      col("age", duckdb::LogicalTypeId::SMALLINT)};
  auto props = make_props(test_file.string(), columns, "", "", true);
  auto logger = mdlog::Logger::CreateNopLogger();
  csv_processor::ProcessFile(
      con, props, logger, [&con](const std::string &staging_table_name) {
        const auto res = con.Query("FROM " + staging_table_name);
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

TEST_CASE("Test reading CSV file with BINARY column", "[csv_processor]") {
  const auto [base64_data, decoded_string] =
      GENERATE(table<std::string, std::string>(
          {std::make_tuple<std::string, std::string>("3q2+7w==",
                                                     R"(\xDE\xAD\xBE\xEF)"),
           std::make_tuple<std::string, std::string>(
               "AAECAwQFBgc=", R"(\x00\x01\x02\x03\x04\x05\x06\x07)"),
           std::make_tuple<std::string, std::string>("SGVsbG8gV29ybGQh",
                                                     "Hello World!"),
           std::make_tuple<std::string, std::string>("", "NULL")}));

  const fs::path temp_csv_file = fs::temp_directory_path() / "temp_binary.csv";
  {
    std::ofstream ofs(temp_csv_file, std::ios::trunc);
    ofs << "binary_val\n";
    ofs << "\"" << base64_data << "\"\n";
    ofs.close();
  }

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  const std::vector<column_def> columns{
      col("binary_val", duckdb::LogicalTypeId::BLOB)};

  auto props = make_props(temp_csv_file.string(), columns);
  auto logger = mdlog::Logger::CreateNopLogger();
  csv_processor::ProcessFile(
      con, props, logger,
      [&con, expected_varchar =
                 decoded_string](const std::string &staging_table_name) {
        const auto res =
            con.Query("SELECT binary_val FROM " + staging_table_name);
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->RowCount() == 1);
        const auto blob_value = res->GetValue(0, 0);
        REQUIRE(blob_value.type().id() == duckdb::LogicalTypeId::BLOB);
        REQUIRE(blob_value.ToString() == expected_varchar);
      });

  fs::remove(temp_csv_file);
}

TEST_CASE("Test reading zstd-compressed CSV files", "[csv_processor]") {
  auto [filename, row_count, columns] = GENERATE(table<std::string, size_t,
                                                       std::vector<column_def>>(
      {std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "customers-100000.csv.zst", 100000,
           {col("Index", duckdb::LogicalTypeId::INTEGER),
            col("Customer Id", duckdb::LogicalTypeId::VARCHAR),
            col("First Name", duckdb::LogicalTypeId::VARCHAR),
            col("Last Name", duckdb::LogicalTypeId::VARCHAR),
            col("Company", duckdb::LogicalTypeId::VARCHAR),
            col("City", duckdb::LogicalTypeId::VARCHAR),
            col("Country", duckdb::LogicalTypeId::VARCHAR),
            col("Phone 1", duckdb::LogicalTypeId::VARCHAR),
            col("Phone 2", duckdb::LogicalTypeId::VARCHAR),
            col("Email", duckdb::LogicalTypeId::VARCHAR),
            col("Subscription Date", duckdb::LogicalTypeId::DATE),
            col("Website", duckdb::LogicalTypeId::VARCHAR)}),
       std::make_tuple<std::string, size_t, std::vector<column_def>>(
           "train_few_shot.csv.zst", 40000,
           {col("text", duckdb::LogicalTypeId::VARCHAR),
            col("label", duckdb::LogicalTypeId::VARCHAR),
            col("source", duckdb::LogicalTypeId::VARCHAR),
            col("reasoning", duckdb::LogicalTypeId::VARCHAR)})}));

  const fs::path test_file =
      fs::path(TEST_RESOURCES_DIR) / "compressed_csv" / filename;
  CAPTURE(test_file);
  REQUIRE(fs::exists(test_file));

  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  auto props = make_props(test_file.string(), columns);
  auto logger = mdlog::Logger::CreateNopLogger();
  csv_processor::ProcessFile(con, props, logger,
                             [&con, expected_row_count = row_count](
                                 const std::string &staging_table_name) {
                               const auto res =
                                   con.Query("FROM " + staging_table_name);
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
          {col("name", duckdb::LogicalTypeId::VARCHAR),
           decimal_col("num", 6, 3),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           pk("_fivetran_id", duckdb::LogicalTypeId::VARCHAR),
           col("_fivetran_deleted", duckdb::LogicalTypeId::BOOLEAN)}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "campaign_input_1_update", 1, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          true,
          {col("name", duckdb::LogicalTypeId::VARCHAR),
           decimal_col("num", 6, 3),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           pk("_fivetran_id", duckdb::LogicalTypeId::VARCHAR),
           col("_fivetran_deleted", duckdb::LogicalTypeId::BOOLEAN)}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "campaign_input_1_delete", 1, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          false,
          {col("name", duckdb::LogicalTypeId::VARCHAR),
           decimal_col("num", 6, 3),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           pk("_fivetran_id", duckdb::LogicalTypeId::VARCHAR),
           col("_fivetran_deleted", duckdb::LogicalTypeId::BOOLEAN)}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "transaction_input_1_upsert", 7,
          "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP", false,
          {pk("id", duckdb::LogicalTypeId::INTEGER),
           col("amount", duckdb::LogicalTypeId::FLOAT),
           col("desc", duckdb::LogicalTypeId::VARCHAR),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           col("_fivetran_deleted", duckdb::LogicalTypeId::BOOLEAN)}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "transaction_input_1_update", 4,
          "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP", true,
          {pk("id", duckdb::LogicalTypeId::INTEGER),
           col("amount", duckdb::LogicalTypeId::FLOAT),
           col("desc", duckdb::LogicalTypeId::VARCHAR),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           col("_fivetran_deleted", duckdb::LogicalTypeId::BOOLEAN)}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "transaction_input_1_delete", 2,
          "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP", false,
          {pk("id", duckdb::LogicalTypeId::INTEGER),
           col("amount", duckdb::LogicalTypeId::FLOAT),
           col("desc", duckdb::LogicalTypeId::VARCHAR),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           col("_fivetran_deleted", duckdb::LogicalTypeId::BOOLEAN)}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "orders_input_2_upsert", 3, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          false,
          {pk("order_id", duckdb::LogicalTypeId::VARCHAR),
           col("customer_id", duckdb::LogicalTypeId::BIGINT),
           col("order_date", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           col("status", duckdb::LogicalTypeId::VARCHAR),
           col("total_amount", duckdb::LogicalTypeId::FLOAT),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           col("_fivetran_deleted", duckdb::LogicalTypeId::BOOLEAN)}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "orders_input_2_update", 3, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          true,
          {pk("order_id", duckdb::LogicalTypeId::VARCHAR),
           col("customer_id", duckdb::LogicalTypeId::BIGINT),
           col("order_date", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           col("status", duckdb::LogicalTypeId::VARCHAR),
           col("total_amount", duckdb::LogicalTypeId::FLOAT),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           col("_fivetran_deleted", duckdb::LogicalTypeId::BOOLEAN)}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "customers_input_2_upsert", 6, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          false,
          {pk("customer_id", duckdb::LogicalTypeId::BIGINT),
           col("first_name", duckdb::LogicalTypeId::VARCHAR),
           col("last_name", duckdb::LogicalTypeId::VARCHAR),
           col("email", duckdb::LogicalTypeId::VARCHAR),
           col("registration_date", duckdb::LogicalTypeId::DATE),
           decimal_col("total_spent", 12, 2),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           col("loyalty_tier", duckdb::LogicalTypeId::VARCHAR)}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "customers_input_2_update", 2, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          true,
          {pk("customer_id", duckdb::LogicalTypeId::BIGINT),
           col("first_name", duckdb::LogicalTypeId::VARCHAR),
           col("last_name", duckdb::LogicalTypeId::VARCHAR),
           col("email", duckdb::LogicalTypeId::VARCHAR),
           col("registration_date", duckdb::LogicalTypeId::DATE),
           decimal_col("total_spent", 12, 2),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           col("loyalty_tier", duckdb::LogicalTypeId::VARCHAR)}),
      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "customers_input_2_delete", 1, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          false,
          {pk("customer_id", duckdb::LogicalTypeId::BIGINT),
           col("first_name", duckdb::LogicalTypeId::VARCHAR),
           col("last_name", duckdb::LogicalTypeId::VARCHAR),
           col("email", duckdb::LogicalTypeId::VARCHAR),
           col("registration_date", duckdb::LogicalTypeId::DATE),
           decimal_col("total_spent", 12, 2),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           col("loyalty_tier", duckdb::LogicalTypeId::VARCHAR)}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "products_input_2_upsert", 5, "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP",
          false,
          {col("product_name", duckdb::LogicalTypeId::VARCHAR),
           col("category", duckdb::LogicalTypeId::VARCHAR),
           col("unit_price", duckdb::LogicalTypeId::DOUBLE),
           col("stock_count", duckdb::LogicalTypeId::INTEGER),
           col("is_active", duckdb::LogicalTypeId::BOOLEAN),
           col("specs_json", duckdb::LogicalTypeId::VARCHAR),
           col("manual_xml", duckdb::LogicalTypeId::VARCHAR),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           pk("_fivetran_id", duckdb::LogicalTypeId::VARCHAR)}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "user_profiles_input_3_upsert", 24,
          "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP", false,
          {pk("user_id", duckdb::LogicalTypeId::BIGINT),
           col("username", duckdb::LogicalTypeId::VARCHAR),
           col("email", duckdb::LogicalTypeId::VARCHAR),
           col("created_at", duckdb::LogicalTypeId::TIMESTAMP),
           col("is_active", duckdb::LogicalTypeId::BOOLEAN),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ)}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "web_events_input_3_upsert", 154,
          "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP", false,
          {pk("event_id", duckdb::LogicalTypeId::VARCHAR),
           col("session_id", duckdb::LogicalTypeId::VARCHAR),
           col("user_id", duckdb::LogicalTypeId::BIGINT),
           col("event_timestamp", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           col("event_type", duckdb::LogicalTypeId::VARCHAR),
           col("url", duckdb::LogicalTypeId::VARCHAR),
           col("ip_address", duckdb::LogicalTypeId::VARCHAR),
           col("user_agent", duckdb::LogicalTypeId::VARCHAR),
           col("payload", duckdb::LogicalTypeId::VARCHAR),
           col("processing_time_ms", duckdb::LogicalTypeId::INTEGER),
           col("is_error", duckdb::LogicalTypeId::BOOLEAN),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ)}),

      std::make_tuple<std::string, size_t, std::string, bool,
                      std::vector<column_def>>(
          "mixed_data_input_4_upsert", 1,
          "null-m8yilkvPsNulehxl2G6pmSQ3G3WWdLP", false,
          {col("bool_val", duckdb::LogicalTypeId::BOOLEAN),
           col("short_val", duckdb::LogicalTypeId::SMALLINT),
           col("int_val", duckdb::LogicalTypeId::INTEGER),
           col("long_val", duckdb::LogicalTypeId::BIGINT),
           col("float_val", duckdb::LogicalTypeId::FLOAT),
           col("double_val", duckdb::LogicalTypeId::DOUBLE),
           decimal_col("decimal_val", 38, 10),
           col("naive_time_val", duckdb::LogicalTypeId::TIME),
           col("naive_date_val", duckdb::LogicalTypeId::DATE),
           col("naive_datetime_val", duckdb::LogicalTypeId::TIMESTAMP),
           col("utc_datetime_val", duckdb::LogicalTypeId::TIMESTAMP_TZ),
           col("binary_val", duckdb::LogicalTypeId::BLOB),
           col("json_val", duckdb::LogicalTypeId::VARCHAR),
           col("string_val", duckdb::LogicalTypeId::VARCHAR),
           col("xml_val", duckdb::LogicalTypeId::VARCHAR),
           col("_fivetran_synced", duckdb::LogicalTypeId::TIMESTAMP_TZ)}),
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

  auto props = make_props(test_file.string(), columns, decryption_key,
                          null_string, can_contain_unmodified);

  auto logger = mdlog::Logger::CreateNopLogger();
  csv_processor::ProcessFile(
      con, props, logger,
      [&con,
       expected_row_count = row_count](const std::string &staging_table_name) {
        const auto res = con.Query("FROM " + staging_table_name);
        if (res->HasError()) {
          FAIL("Failed to execute \"FROM csv_view\" query: " + res->GetError());
        }
        REQUIRE_FALSE(res->HasError());
        REQUIRE(res->RowCount() == expected_row_count);
      });
}

TEST_CASE("Test reading a CSV file with a huge VARCHAR column",
          "[csv_processor]") {
  SECTION("Fails to read a CSV file with a 20 MB VARCHAR column") {
    const fs::path test_file = fs::path(TEST_RESOURCES_DIR) / "compressed_csv" /
                               "lorem_ipsum_20mb.csv.zst";
    REQUIRE(fs::exists(test_file));

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    auto props = make_props(test_file.string(),
                            {col("id", duckdb::LogicalTypeId::INTEGER),
                             col("text", duckdb::LogicalTypeId::VARCHAR)});
    auto logger = mdlog::Logger::CreateNopLogger();
    REQUIRE_THROWS_WITH(csv_processor::ProcessFile(con, props, logger,
                                                   [](const std::string &) {
                                                     // Do nothing
                                                   }),
                        Catch::Matchers::ContainsSubstring(
                            "Maximum line size of 16777216 bytes exceeded"));
  }

  SECTION("Reading a CSV file with a 15 MB VARCHAR column succeeds") {
    const fs::path test_file = fs::path(TEST_RESOURCES_DIR) / "compressed_csv" /
                               "lorem_ipsum_15mb.csv.zst";
    REQUIRE(fs::exists(test_file));

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    auto props = make_props(test_file.string(),
                            {col("id", duckdb::LogicalTypeId::INTEGER),
                             col("text", duckdb::LogicalTypeId::VARCHAR)});
    auto logger = mdlog::Logger::CreateNopLogger();
    csv_processor::ProcessFile(
        con, props, logger, [&con](const std::string &staging_table_name) {
          const auto res = con.Query("FROM " + staging_table_name);
          if (res->HasError()) {
            FAIL("Failed to execute \"FROM csv_view\" query: " +
                 res->GetError());
          }
          REQUIRE_FALSE(res->HasError());
          REQUIRE(res->RowCount() == 1);
        });
  }
}
