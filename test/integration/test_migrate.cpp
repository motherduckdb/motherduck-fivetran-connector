#include "../constants.hpp"
#include "common.hpp"
#include "config_tester.hpp"
#include "duckdb.hpp"
#include "motherduck_destination_server.hpp"

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <fstream>
#include <future>
#include <thread>
#include <vector>

using namespace test::constants;

TEST_CASE("Migrate - drop table", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_drop_table_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  {
    ::fivetran_sdk::v2::CreateTableRequest request;
    add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
    add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, false);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    auto res = con->Query("INSERT INTO " + table_name + " VALUES (1, 'Alice')");
    REQUIRE_NO_FAIL(res);
  }

  {
    auto res = con->Query("SELECT COUNT(*) FROM " + table_name);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->GetValue(0, 0) == 1);
  }

  // Drop the table using Migrate
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    request.mutable_details()->mutable_drop()->set_drop_table(true);

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify table no longer exists
  {
    ::fivetran_sdk::v2::DescribeTableRequest request;
    add_config(request, MD_TOKEN, TEST_DATABASE_NAME);
    request.set_table_name(table_name);

    ::fivetran_sdk::v2::DescribeTableResponse response;
    auto status = service.DescribeTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.not_found());
  }
}

TEST_CASE("Migrate - rename table", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string from_table =
      "migrate_rename_from_" + std::to_string(Catch::rngSeed());
  const std::string to_table =
      "migrate_rename_to_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create the source table
  {
    ::fivetran_sdk::v2::CreateTableRequest request;
    add_config(request, MD_TOKEN, TEST_DATABASE_NAME, from_table);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
    add_col(request, "value", ::fivetran_sdk::v2::DataType::STRING, false);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  // Insert data
  {
    auto res =
        con->Query("INSERT INTO " + from_table + " VALUES (1, 'test_data')");
    REQUIRE_NO_FAIL(res);
  }

  // Rename the table
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(from_table);
    request.mutable_details()
        ->mutable_rename()
        ->mutable_rename_table()
        ->set_from_table(from_table);
    request.mutable_details()
        ->mutable_rename()
        ->mutable_rename_table()
        ->set_to_table(to_table);

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify old table doesn't exist
  {
    ::fivetran_sdk::v2::DescribeTableRequest request;
    add_config(request, MD_TOKEN, TEST_DATABASE_NAME);
    request.set_table_name(from_table);

    ::fivetran_sdk::v2::DescribeTableResponse response;
    auto status = service.DescribeTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.not_found());
  }

  // Verify new table exists with data
  {
    auto res = con->Query("SELECT value FROM " + to_table + " WHERE id = 1");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0).ToString() == "test_data");
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + to_table);
}

TEST_CASE("Migrate - rename column", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_rename_col_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create table
  {
    ::fivetran_sdk::v2::CreateTableRequest request;
    add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
    add_col(request, "old_name", ::fivetran_sdk::v2::DataType::STRING, false);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  // Insert data
  {
    auto res =
        con->Query("INSERT INTO " + table_name + " VALUES (1, 'test_value')");
    REQUIRE_NO_FAIL(res);
  }

  // Rename column
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    request.mutable_details()
        ->mutable_rename()
        ->mutable_rename_column()
        ->set_from_column("old_name");
    request.mutable_details()
        ->mutable_rename()
        ->mutable_rename_column()
        ->set_to_column("new_name");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify column was renamed and data preserved
  {
    auto res =
        con->Query("SELECT new_name FROM " + table_name + " WHERE id = 1");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0).ToString() == "test_value");
  }

  // Verify old column name doesn't work
  {
    auto res = con->Query("SELECT old_name FROM " + table_name);
    REQUIRE(res->HasError());
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - copy table", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string from_table =
      "migrate_copy_from_" + std::to_string(Catch::rngSeed());
  const std::string to_table =
      "migrate_copy_to_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create the source table
  {
    ::fivetran_sdk::v2::CreateTableRequest request;
    add_config(request, MD_TOKEN, TEST_DATABASE_NAME, from_table);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
    add_col(request, "data", ::fivetran_sdk::v2::DataType::STRING, false);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  // Insert data
  {
    auto res = con->Query("INSERT INTO " + from_table +
                          " VALUES (1, 'data1'), (2, 'data2')");
    REQUIRE_NO_FAIL(res);
  }

  // Copy the table
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(from_table);
    request.mutable_details()
        ->mutable_copy()
        ->mutable_copy_table()
        ->set_from_table(from_table);
    request.mutable_details()
        ->mutable_copy()
        ->mutable_copy_table()
        ->set_to_table(to_table);

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify both tables exist with correct data
  {
    auto res = con->Query("SELECT COUNT(*) FROM " + from_table);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->GetValue(0, 0) == 2);
  }
  {
    auto res = con->Query("SELECT COUNT(*) FROM " + to_table);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->GetValue(0, 0) == 2);
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + from_table);
  con->Query("DROP TABLE IF EXISTS " + to_table);
}

TEST_CASE("Migrate - copy column", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_copy_col_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create table
  {
    ::fivetran_sdk::v2::CreateTableRequest request;
    add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
    add_col(request, "source_col", ::fivetran_sdk::v2::DataType::STRING, false);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  // Insert data
  {
    auto res =
        con->Query("INSERT INTO " + table_name + " VALUES (1, 'original')");
    REQUIRE_NO_FAIL(res);
  }

  // Copy column
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    request.mutable_details()
        ->mutable_copy()
        ->mutable_copy_column()
        ->set_from_column("source_col");
    request.mutable_details()
        ->mutable_copy()
        ->mutable_copy_column()
        ->set_to_column("dest_col");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify both columns exist with same data
  {
    auto res = con->Query("SELECT source_col, dest_col FROM " + table_name +
                          " WHERE id = 1");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0).ToString() == "original");
    REQUIRE(res->GetValue(1, 0).ToString() == "original");
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - copy table to history mode from soft delete",
          "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string source_table =
      "migrate_copy_hist_src_" + std::to_string(Catch::rngSeed());
  const std::string dest_table =
      "migrate_copy_hist_dst_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create source table with soft delete column
  con->Query("DROP TABLE IF EXISTS " + source_table);
  con->Query("DROP TABLE IF EXISTS " + dest_table);
  {
    auto res = con->Query("CREATE TABLE " + source_table +
                          " (id INT, name VARCHAR, _fivetran_deleted BOOLEAN, "
                          "_fivetran_synced TIMESTAMPTZ, primary key (id))");
    REQUIRE_NO_FAIL(res);
  }

  // Insert data with some deleted rows
  {
    auto res = con->Query("INSERT INTO " + source_table +
                          " VALUES (1, 'Alice', false, NOW()), "
                          "(2, 'Bob', true, NOW()), "
                          "(3, 'Charlie', false, NOW())");
    REQUIRE_NO_FAIL(res);
  }

  // Copy to history mode
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(source_table);
    auto *copy_hist = request.mutable_details()
                          ->mutable_copy()
                          ->mutable_copy_table_to_history_mode();
    copy_hist->set_from_table(source_table);
    copy_hist->set_to_table(dest_table);
    copy_hist->set_soft_deleted_column("_fivetran_deleted");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify destination table has history columns
  {
    auto res = con->Query("SELECT id, name, _fivetran_active FROM " +
                          dest_table + " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 3);
    // Alice (not deleted) -> active
    REQUIRE(res->GetValue(2, 0) == true);
    // Bob (deleted) -> not active
    REQUIRE(res->GetValue(2, 1) == false);
    // Charlie (not deleted) -> active
    REQUIRE(res->GetValue(2, 2) == true);
  }

  // Verify soft_deleted_column is NOT in destination
  {
    auto res = con->Query("SELECT _fivetran_deleted FROM " + dest_table);
    REQUIRE(res->HasError());
  }

  // Verify history columns exist
  {
    auto res =
        con->Query("SELECT _fivetran_start, _fivetran_end FROM " + dest_table);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 3);
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + source_table);
  con->Query("DROP TABLE IF EXISTS " + dest_table);
}

TEST_CASE("Migrate - copy table to history mode from live",
          "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string source_table =
      "migrate_copy_hist_live_src_" + std::to_string(Catch::rngSeed());
  const std::string dest_table =
      "migrate_copy_hist_live_dst_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create source table (live mode - no soft delete column)
  con->Query("DROP TABLE IF EXISTS " + source_table);
  con->Query("DROP TABLE IF EXISTS " + dest_table);
  {
    auto res =
        con->Query("CREATE TABLE " + source_table + " (id INT, name VARCHAR)");
    REQUIRE_NO_FAIL(res);
  }

  // Insert data
  {
    auto res = con->Query("INSERT INTO " + source_table +
                          " VALUES (1, 'Alice'), (2, 'Bob')");
    REQUIRE_NO_FAIL(res);
  }

  // Copy to history mode (no soft_deleted_column)
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(source_table);
    auto *copy_hist = request.mutable_details()
                          ->mutable_copy()
                          ->mutable_copy_table_to_history_mode();
    copy_hist->set_from_table(source_table);
    copy_hist->set_to_table(dest_table);
    // No soft_deleted_column set - this is a live table

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify destination table has history columns and all rows are active
  {
    auto res = con->Query("SELECT id, name, _fivetran_active FROM " +
                          dest_table + " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);
    // All rows should be active (live mode)
    REQUIRE(res->GetValue(2, 0) == true);
    REQUIRE(res->GetValue(2, 1) == true);
  }

  // Verify history columns exist with proper values
  {
    auto res =
        con->Query("SELECT _fivetran_end FROM " + dest_table +
                   " WHERE _fivetran_end = '9999-12-31 23:59:59'::TIMESTAMPTZ");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + source_table);
  con->Query("DROP TABLE IF EXISTS " + dest_table);
}

TEST_CASE("Migrate - add column with default value", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_add_col_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create table
  {
    ::fivetran_sdk::v2::CreateTableRequest request;
    add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  // Insert data
  {
    auto res = con->Query("INSERT INTO " + table_name + " VALUES (1)");
    REQUIRE_NO_FAIL(res);
  }

  // Add column with default
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    auto add_col = request.mutable_details()
                       ->mutable_add()
                       ->mutable_add_column_with_default_value();
    add_col->set_column("new_col");
    add_col->set_column_type(::fivetran_sdk::v2::DataType::STRING);
    add_col->set_default_value("'default_value'");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify column exists (new rows get default)
  {
    auto res = con->Query("INSERT INTO " + table_name + " (id) VALUES (2)");
    REQUIRE_NO_FAIL(res);
  }
  {
    auto res =
        con->Query("SELECT new_col FROM " + table_name + " WHERE id = 2");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0).ToString() == "'default_value'");
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - update column value", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_update_col_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create table
  {
    ::fivetran_sdk::v2::CreateTableRequest request;
    add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
    add_col(request, "status", ::fivetran_sdk::v2::DataType::STRING, false);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  // Insert data
  {
    auto res = con->Query("INSERT INTO " + table_name +
                          " VALUES (1, 'old'), (2, 'old'), (3, 'old')");
    REQUIRE_NO_FAIL(res);
  }

  // Update all values in column
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    request.mutable_details()->mutable_update_column_value()->set_column(
        "status");
    request.mutable_details()->mutable_update_column_value()->set_value(
        "'updated'");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify all rows updated
  {
    auto res = con->Query("SELECT COUNT(*) FROM " + table_name +
                          " WHERE status = 'updated'");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->GetValue(0, 0) == 3);
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - add column in history mode", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_add_col_hist_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create a history table manually
  con->Query("DROP TABLE IF EXISTS " + table_name);
  {
    auto res = con->Query("CREATE TABLE " + table_name +
                          " (id INT, name VARCHAR, "
                          "_fivetran_start TIMESTAMPTZ, "
                          "_fivetran_end TIMESTAMPTZ, "
                          "_fivetran_active BOOLEAN)");
    REQUIRE_NO_FAIL(res);
  }

  // Insert active row
  {
    auto res = con->Query("INSERT INTO " + table_name +
                          " VALUES (1, 'Alice', '2024-01-01'::TIMESTAMPTZ, "
                          "'9999-12-31 23:59:59'::TIMESTAMPTZ, true)");
    REQUIRE_NO_FAIL(res);
  }

  // Add column in history mode
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    auto *add_col = request.mutable_details()
                        ->mutable_add()
                        ->mutable_add_column_in_history_mode();
    add_col->set_column("age");
    add_col->set_column_type(::fivetran_sdk::v2::DataType::INT);
    add_col->set_default_value("25");
    add_col->set_operation_timestamp("2024-06-01T00:00:00Z");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }
  // Add another column in history mode with the same operation timestamp
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    auto *add_col = request.mutable_details()
                        ->mutable_add()
                        ->mutable_add_column_in_history_mode();
    add_col->set_column("switch");
    add_col->set_column_type(::fivetran_sdk::v2::DataType::BOOLEAN);
    add_col->set_default_value("false");
    add_col->set_operation_timestamp("2024-06-01T00:00:00Z");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify: should have 2 rows now (old inactive + new active)
  {
    auto res = con->Query("SELECT COUNT(*) FROM " + table_name);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->GetValue(0, 0) == 2);
  }

  // Verify: new active row has the new column with default value
  {
    auto res = con->Query("SELECT age FROM " + table_name +
                          " WHERE _fivetran_active = TRUE");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0) == 25);
  }
  // Verify: new active row has the new column with default value
  {
    auto res = con->Query("SELECT switch FROM " + table_name +
                          " WHERE _fivetran_active = TRUE");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0) == false);
  }

  // Verify: old row is now inactive
  {
    auto res = con->Query("SELECT _fivetran_active FROM " + table_name +
                          " WHERE _fivetran_start = '2024-01-01'::TIMESTAMPTZ");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0) == false);
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - drop column in history mode", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_drop_col_hist_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create a history table manually with an extra column
  con->Query("DROP TABLE IF EXISTS " + table_name);
  {
    auto res = con->Query("CREATE TABLE " + table_name +
                          " (id INT, name VARCHAR, email VARCHAR, "
                          "_fivetran_start TIMESTAMPTZ, "
                          "_fivetran_end TIMESTAMPTZ, "
                          "_fivetran_active BOOLEAN)");
    REQUIRE_NO_FAIL(res);
  }

  // Insert active row
  {
    auto res = con->Query(
        "INSERT INTO " + table_name +
        " VALUES (1, 'Alice', 'alice@example.com', '2024-01-01'::TIMESTAMPTZ, "
        "'9999-12-31 23:59:59'::TIMESTAMPTZ, true)");
    REQUIRE_NO_FAIL(res);
  }

  // Drop column in history mode
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    auto *drop_col = request.mutable_details()
                         ->mutable_drop()
                         ->mutable_drop_column_in_history_mode();
    drop_col->set_column("email");
    drop_col->set_operation_timestamp("2024-06-01T00:00:00Z");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify: should have 2 rows now (old inactive + new active)
  {
    auto res = con->Query("SELECT COUNT(*) FROM " + table_name);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->GetValue(0, 0) == 2);
  }

  // Verify: new active row has NULL for the dropped column
  {
    auto res = con->Query("SELECT email FROM " + table_name +
                          " WHERE _fivetran_active = TRUE");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0).IsNull());
  }

  // Verify: old row is now inactive but still has email value
  {
    auto res = con->Query("SELECT email, _fivetran_active FROM " + table_name +
                          " WHERE _fivetran_start = '2024-01-01'::TIMESTAMPTZ");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0).ToString() == "alice@example.com");
    REQUIRE(res->GetValue(1, 0) == false);
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - live to soft delete", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_live_soft_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create a "live" table (no soft delete column)
  {
    ::fivetran_sdk::v2::CreateTableRequest request;
    add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
    add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, false);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  // Insert data
  {
    auto res = con->Query("INSERT INTO " + table_name +
                          " VALUES (1, 'Alice'), (2, 'Bob')");
    REQUIRE_NO_FAIL(res);
  }

  // Migrate to soft delete mode
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    request.mutable_details()->mutable_table_sync_mode_migration()->set_type(
        ::fivetran_sdk::v2::LIVE_TO_SOFT_DELETE);
    request.mutable_details()
        ->mutable_table_sync_mode_migration()
        ->set_soft_deleted_column("_fivetran_deleted");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify _fivetran_deleted column exists and all rows are not deleted
  {
    auto res = con->Query("SELECT id, _fivetran_deleted FROM " + table_name +
                          " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);
    REQUIRE(res->GetValue(1, 0) == false);
    REQUIRE(res->GetValue(1, 1) == false);
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - soft delete to live", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_soft_live_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create a table with soft delete column
  {
    auto res = con->Query("CREATE TABLE " + table_name +
                          " (id INT PRIMARY KEY, name VARCHAR, "
                          "_fivetran_deleted BOOLEAN)");
    REQUIRE_NO_FAIL(res);
  }

  // Insert data with some deleted rows
  {
    auto res = con->Query("INSERT INTO " + table_name +
                          " VALUES (1, 'Alice', false), "
                          "(2, 'Bob', true), "
                          "(3, 'Charlie', false)");
    REQUIRE_NO_FAIL(res);
  }

  // Migrate to live mode (removes deleted rows and column)
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    request.mutable_details()->mutable_table_sync_mode_migration()->set_type(
        ::fivetran_sdk::v2::SOFT_DELETE_TO_LIVE);
    request.mutable_details()
        ->mutable_table_sync_mode_migration()
        ->set_soft_deleted_column("_fivetran_deleted");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify deleted row is gone
  {
    auto res =
        con->Query("SELECT id, name FROM " + table_name + " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);
    REQUIRE(res->GetValue(0, 0) == 1);
    REQUIRE(res->GetValue(0, 1) == 3);
  }

  // Verify _fivetran_deleted column is gone
  {
    auto res = con->Query("SELECT _fivetran_deleted FROM " + table_name);
    REQUIRE(res->HasError());
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - live to history", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_live_hist_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create a live table
  {
    ::fivetran_sdk::v2::CreateTableRequest request;
    add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
    add_col(request, "value", ::fivetran_sdk::v2::DataType::STRING, false);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  // Insert data
  {
    auto res =
        con->Query("INSERT INTO " + table_name + " VALUES (1, 'initial')");
    REQUIRE_NO_FAIL(res);
  }

  // Migrate to history mode
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    request.mutable_details()->mutable_table_sync_mode_migration()->set_type(
        ::fivetran_sdk::v2::LIVE_TO_HISTORY);

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify history columns exist and record is active
  {
    auto res = con->Query("SELECT id, value, _fivetran_start, _fivetran_end, "
                          "_fivetran_active FROM " +
                          table_name);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0) == 1);
    REQUIRE(res->GetValue(1, 0).ToString() == "initial");
    REQUIRE_FALSE(res->GetValue(2, 0).IsNull()); // _fivetran_start is set
    REQUIRE_FALSE(res->GetValue(3, 0).IsNull()); // _fivetran_end is set
    REQUIRE(res->GetValue(4, 0) == true);        // _fivetran_active = true
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - history to live", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_hist_live_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Drop and create a history table manually (no primary key to allow duplicate
  // ids)
  con->Query("DROP TABLE IF EXISTS " + table_name);
  {
    auto res = con->Query("CREATE TABLE " + table_name +
                          " (id INT, value VARCHAR, "
                          "_fivetran_start TIMESTAMPTZ, "
                          "_fivetran_end TIMESTAMPTZ, "
                          "_fivetran_active BOOLEAN, "
                          "primary key (id, _fivetran_start))");
    REQUIRE_NO_FAIL(res);
  }

  // Insert data with active and inactive records (same id can appear multiple
  // times in history)
  {
    auto res =
        con->Query("INSERT INTO " + table_name +
                   " VALUES (1, 'current', NOW(), '9999-12-31 "
                   "23:59:59'::TIMESTAMPTZ, true),"
                   "(1, 'old', '2020-01-01'::TIMESTAMPTZ, NOW(), false),"
                   "(2, 'deleted', '2020-01-01'::TIMESTAMPTZ, NOW(), false)");
    REQUIRE_NO_FAIL(res);
  }

  // Migrate to live mode (keep_deleted_rows = false)
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    request.mutable_details()->mutable_table_sync_mode_migration()->set_type(
        ::fivetran_sdk::v2::HISTORY_TO_LIVE);
    request.mutable_details()
        ->mutable_table_sync_mode_migration()
        ->set_keep_deleted_rows(false);

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify only active record remains
  {
    auto res = con->Query("SELECT id, value FROM " + table_name);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0) == 1);
    REQUIRE(res->GetValue(1, 0).ToString() == "current");
  }

  // Verify history columns are gone
  {
    auto res = con->Query("SELECT _fivetran_start FROM " + table_name);
    REQUIRE(res->HasError());
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - history to soft delete", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_hist_soft_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Drop and create a history table manually (no primary key to allow duplicate
  // ids)
  con->Query("DROP TABLE IF EXISTS " + table_name);
  {
    auto res = con->Query("CREATE TABLE " + table_name +
                          " (id INT, id2 INT, value VARCHAR default 'abc', "
                          "_fivetran_start TIMESTAMPTZ, "
                          "_fivetran_end TIMESTAMPTZ, "
                          "_fivetran_active BOOLEAN,"
                          "primary key (id, id2, _fivetran_start))");
    REQUIRE_NO_FAIL(res);
  }

  // Insert data with active and inactive records
  {
    auto res = con->Query(
        "INSERT INTO " + table_name +
        " VALUES (1, 1, 'active_row', NOW(), '9999-12-31 "
        "23:59:59'::TIMESTAMPTZ, true),"
        "(1, 1, 'inactive_row', '2020-01-01'::TIMESTAMPTZ, NOW(), false), "
        "(2, 1, 'active_row', NOW(), '9999-12-31 23:59:59'::TIMESTAMPTZ, "
        "false)");
    REQUIRE_NO_FAIL(res);
  }

  // Migrate to soft delete mode
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    request.mutable_details()->mutable_table_sync_mode_migration()->set_type(
        ::fivetran_sdk::v2::HISTORY_TO_SOFT_DELETE);
    request.mutable_details()
        ->mutable_table_sync_mode_migration()
        ->set_soft_deleted_column("_fivetran_deleted");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify _fivetran_deleted column exists with correct values
  {
    auto res = con->Query("SELECT id, value, _fivetran_deleted FROM " +
                          table_name + " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);
    REQUIRE(res->GetValue(2, 0) == false); // id=1 was active, so not deleted
    REQUIRE(res->GetValue(2, 1) == true);  // id=2 was inactive, so deleted
  }

  // Verify history columns are gone
  {
    auto res = con->Query("SELECT _fivetran_start FROM " + table_name);
    REQUIRE(res->HasError());
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - soft delete to history", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_soft_hist_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create a table with soft delete column
  {
    auto res =
        con->Query("CREATE TABLE " + table_name +
                   " (id INT PRIMARY KEY, name VARCHAR, "
                   "_fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMPTZ)");
    REQUIRE_NO_FAIL(res);
  }

  // Insert data
  {
    auto res = con->Query("INSERT INTO " + table_name +
                          " VALUES (1, 'active', false, NOW()), "
                          "(2, 'deleted', true, NOW())");
    REQUIRE_NO_FAIL(res);
  }

  // Migrate to history mode
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    request.mutable_details()->mutable_table_sync_mode_migration()->set_type(
        ::fivetran_sdk::v2::SOFT_DELETE_TO_HISTORY);
    request.mutable_details()
        ->mutable_table_sync_mode_migration()
        ->set_soft_deleted_column("_fivetran_deleted");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify history columns exist with correct values
  {
    auto res = con->Query("SELECT id, name, _fivetran_active FROM " +
                          table_name + " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);
    REQUIRE(res->GetValue(2, 0) == true);  // id=1 was not deleted, so active
    REQUIRE(res->GetValue(2, 1) == false); // id=2 was deleted, so not active
  }

  // Verify _fivetran_deleted column is gone
  {
    auto res = con->Query("SELECT _fivetran_deleted FROM " + table_name);
    REQUIRE(res->HasError());
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - fails with empty table name", "[integration][migrate]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::v2::MigrateRequest request;
  (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
  (*request.mutable_configuration())["motherduck_database"] =
      TEST_DATABASE_NAME;
  request.mutable_details()->set_table("");
  request.mutable_details()->mutable_drop()->set_drop_table(true);

  ::fivetran_sdk::v2::MigrateResponse response;
  auto status = service.Migrate(nullptr, &request, &response);
  REQUIRE_FALSE(status.ok());
  CHECK_THAT(status.error_message(),
             Catch::Matchers::ContainsSubstring("Table name cannot be empty"));
}

TEST_CASE("Migrate - unsupported operation returns unsupported",
          "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string table_name =
      "migrate_unsupported_" + std::to_string(Catch::rngSeed());

  // Create table first
  {
    ::fivetran_sdk::v2::CreateTableRequest request;
    add_config(request, MD_TOKEN, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  // Try empty copy operation (unsupported - no specific copy type set)
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_table(table_name);
    // Create an empty copy operation - doesn't set copy_table, copy_column,
    // or copy_table_to_history_mode
    request.mutable_details()->mutable_copy();

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.unsupported());
  }

  // Clean up
  auto con = get_test_connection(MD_TOKEN);
  con->Query("DROP TABLE IF EXISTS " + table_name);
}

TEST_CASE("Migrate - works with schema", "[integration][migrate]") {
  DestinationSdkImpl service;
  const std::string schema_name =
      "migrate_schema_" + std::to_string(Catch::rngSeed());
  const std::string table_name =
      "migrate_table_" + std::to_string(Catch::rngSeed());

  auto con = get_test_connection(MD_TOKEN);

  // Create schema and table
  {
    auto res = con->Query("CREATE SCHEMA IF NOT EXISTS " + schema_name);
    REQUIRE_NO_FAIL(res);
  }
  {
    auto res = con->Query("CREATE TABLE " + schema_name + "." + table_name +
                          " (id INT PRIMARY KEY, value VARCHAR)");
    REQUIRE_NO_FAIL(res);
  }
  {
    auto res = con->Query("INSERT INTO " + schema_name + "." + table_name +
                          " VALUES (1, 'test')");
    REQUIRE_NO_FAIL(res);
  }

  // Rename column using schema
  {
    ::fivetran_sdk::v2::MigrateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = MD_TOKEN;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_details()->set_schema(schema_name);
    request.mutable_details()->set_table(table_name);
    request.mutable_details()
        ->mutable_rename()
        ->mutable_rename_column()
        ->set_from_column("value");
    request.mutable_details()
        ->mutable_rename()
        ->mutable_rename_column()
        ->set_to_column("new_value");

    ::fivetran_sdk::v2::MigrateResponse response;
    auto status = service.Migrate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(response.success());
  }

  // Verify column was renamed
  {
    auto res = con->Query("SELECT new_value FROM " + schema_name + "." +
                          table_name + " WHERE id = 1");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0).ToString() == "test");
  }

  // Clean up
  con->Query("DROP TABLE IF EXISTS " + schema_name + "." + table_name);
  con->Query("DROP SCHEMA IF EXISTS " + schema_name);
}
