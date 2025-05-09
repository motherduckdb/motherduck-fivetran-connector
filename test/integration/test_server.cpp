#include "duckdb.hpp"
#include "extension_helper.hpp"
#include "motherduck_destination_server.hpp"
#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>
#include <fstream>

#define STRING(x) #x
#define XSTRING(s) STRING(s)
const std::string TEST_RESOURCES_DIR = XSTRING(TEST_RESOURCES_LOCATION);

static const std::string TEST_DATABASE_NAME = "fivetran_test010";

bool NO_FAIL(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result) {
  if (result->HasError()) {
    fprintf(stderr, "Query failed with message: %s\n",
            result->GetError().c_str());
  }
  return !result->HasError();
}

bool NO_FAIL(const grpc::Status &status) {
  if (!status.ok()) {
    UNSCOPED_INFO("Query failed with message: " + status.error_message());
  }
  return status.ok();
}

bool REQUIRE_FAIL(const grpc::Status &status,
                  const std::string &expected_error) {
  if (!status.ok()) {
    REQUIRE(status.error_message() == expected_error);
    return true;
  }
  return false;
}

#define REQUIRE_NO_FAIL(result) REQUIRE(NO_FAIL((result)))

class testRunListener : public Catch::EventListenerBase {
public:
  using Catch::EventListenerBase::EventListenerBase;

  void testRunStarting(Catch::TestRunInfo const &) override {
    preload_extensions();
  }
};

CATCH_REGISTER_LISTENER(testRunListener)

TEST_CASE("ConfigurationForm", "[integration][config]") {
  DestinationSdkImpl service;
  ::fivetran_sdk::v2::ConfigurationFormRequest request;
  ::fivetran_sdk::v2::ConfigurationFormResponse response;

  auto status = service.ConfigurationForm(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);

  REQUIRE(response.fields_size() == 3);
  REQUIRE(response.fields(0).name() == "motherduck_token");
  REQUIRE(response.fields(1).name() == "motherduck_database");
  REQUIRE(response.fields(2).name() == "motherduck_csv_block_size");
  REQUIRE(response.fields(2).label() ==
          "Maximum individual value size, in megabytes (default 1 MB)");

  REQUIRE(response.tests_size() == 2);
  REQUIRE(response.tests(0).name() == CONFIG_TEST_NAME_CSV_BLOCK_SIZE);
  REQUIRE(response.tests(0).label() == "Maximum value size is a valid number");
  REQUIRE(response.tests(1).name() == CONFIG_TEST_NAME_AUTHENTICATE);
  REQUIRE(response.tests(1).label() == "Test Authentication");
}

TEST_CASE("DescribeTable fails when database missing", "[integration]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::v2::DescribeTableRequest request;
  (*request.mutable_configuration())["motherduck_token"] = "12345";

  ::fivetran_sdk::v2::DescribeTableResponse response;

  auto status = service.DescribeTable(nullptr, &request, &response);
  REQUIRE_FAIL(status, "Missing property motherduck_database");
}

TEST_CASE("DescribeTable on nonexistent table", "[integration]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::v2::DescribeTableRequest request;
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] =
      TEST_DATABASE_NAME;
  request.set_table_name("nonexistent_table");
  ::fivetran_sdk::v2::DescribeTableResponse response;

  auto status = service.DescribeTable(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);
  REQUIRE(response.not_found());
}

TEST_CASE("CreateTable, DescribeTable for existing table, AlterTable",
          "[integration]") {
  DestinationSdkImpl service;

  const std::string schema_name =
      "some_schema" + std::to_string(Catch::rngSeed());
  const std::string table_name =
      "some_table" + std::to_string(Catch::rngSeed());
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  {
    // Create Table
    ::fivetran_sdk::v2::CreateTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_schema_name(schema_name);
    request.mutable_table()->set_name(table_name);
    auto col1 = request.mutable_table()->add_columns();
    col1->set_name("id");
    col1->set_type(::fivetran_sdk::v2::DataType::STRING);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe the created table
    ::fivetran_sdk::v2::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);

    {
      // table not found in default "main" schema
      ::fivetran_sdk::v2::DescribeTableResponse response;
      auto status = service.DescribeTable(nullptr, &request, &response);
      REQUIRE_NO_FAIL(status);
      REQUIRE(response.not_found());
    }

    {
      // table found in the correct schema
      request.set_schema_name(schema_name);
      ::fivetran_sdk::v2::DescribeTableResponse response;
      auto status = service.DescribeTable(nullptr, &request, &response);
      REQUIRE_NO_FAIL(status);
      REQUIRE(!response.not_found());

      REQUIRE(response.table().name() == table_name);
      REQUIRE(response.table().columns_size() == 1);
      REQUIRE(response.table().columns(0).name() == "id");
      REQUIRE(response.table().columns(0).type() ==
              ::fivetran_sdk::v2::DataType::STRING);
      REQUIRE_FALSE(response.table().columns(0).has_params());
      REQUIRE_FALSE(response.table().columns(0).params().has_decimal());
    }
  }

  {
    // Alter Table
    ::fivetran_sdk::v2::AlterTableRequest request;

    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_schema_name(schema_name);
    request.mutable_table()->set_name(table_name);
    ::fivetran_sdk::v2::Column col1;
    col1.set_name("id");
    col1.set_type(::fivetran_sdk::v2::DataType::INT);
    request.mutable_table()->add_columns()->CopyFrom(col1);

    ::fivetran_sdk::v2::AlterTableResponse response;
    auto status = service.AlterTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe the altered table
    ::fivetran_sdk::v2::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);

    // table found in the correct schema
    request.set_schema_name(schema_name);
    ::fivetran_sdk::v2::DescribeTableResponse response;
    auto status = service.DescribeTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(!response.not_found());

    REQUIRE(response.table().name() == table_name);
    REQUIRE(response.table().columns_size() == 1);
    REQUIRE(response.table().columns(0).name() == "id");
    REQUIRE(response.table().columns(0).type() ==
            ::fivetran_sdk::v2::DataType::INT);
  }
}

TEST_CASE("Test fails when database missing", "[integration][configtest]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::v2::TestRequest request;
  (*request.mutable_configuration())["motherduck_token"] = "12345";

  ::fivetran_sdk::v2::TestResponse response;

  auto status = service.Test(nullptr, &request, &response);
  REQUIRE_FAIL(status, "Missing property motherduck_database");
}

TEST_CASE("Test fails when token is missing", "[integration][configtest]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::v2::TestRequest request;
  request.set_name(CONFIG_TEST_NAME_AUTHENTICATE);
  (*request.mutable_configuration())["motherduck_database"] =
      TEST_DATABASE_NAME;

  ::fivetran_sdk::v2::TestResponse response;

  auto status = service.Test(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);
  auto expected_message = "Test <test_authentication> for database <" +
                          TEST_DATABASE_NAME +
                          "> failed: Missing "
                          "property motherduck_token";
  REQUIRE(status.error_message() == expected_message);
  REQUIRE(response.failure() == expected_message);
}

TEST_CASE("Test endpoint fails when token is bad",
          "[integration][configtest]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::v2::TestRequest request;
  request.set_name(CONFIG_TEST_NAME_AUTHENTICATE);
  (*request.mutable_configuration())["motherduck_database"] =
      TEST_DATABASE_NAME;
  (*request.mutable_configuration())["motherduck_token"] = "12345";

  ::fivetran_sdk::v2::TestResponse response;

  auto status = service.Test(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);
  CHECK_THAT(status.error_message(),
             Catch::Matchers::ContainsSubstring("not authenticated"));
}

TEST_CASE(
    "Test endpoint authentication test succeeds when everything is in order",
    "[integration][configtest]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::v2::TestRequest request;
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);
  request.set_name(CONFIG_TEST_NAME_AUTHENTICATE);
  (*request.mutable_configuration())["motherduck_database"] =
      TEST_DATABASE_NAME;
  (*request.mutable_configuration())["motherduck_token"] = token;

  ::fivetran_sdk::v2::TestResponse response;

  auto status = service.Test(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);
}

TEST_CASE("Test endpoint block size validation succeeds when optional block "
          "size is missing",
          "[integration][configtest]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::v2::TestRequest request;
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);
  request.set_name(CONFIG_TEST_NAME_CSV_BLOCK_SIZE);
  (*request.mutable_configuration())["motherduck_database"] =
      TEST_DATABASE_NAME;
  (*request.mutable_configuration())["motherduck_token"] = token;

  ::fivetran_sdk::v2::TestResponse response;

  auto status = service.Test(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);
}

TEST_CASE("Test endpoint block size validation succeeds when optional block "
          "size is a valid number",
          "[integration][configtest]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::v2::TestRequest request;
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);
  request.set_name(CONFIG_TEST_NAME_CSV_BLOCK_SIZE);
  (*request.mutable_configuration())["motherduck_database"] =
      TEST_DATABASE_NAME;
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())[MD_PROP_CSV_BLOCK_SIZE] = "5";

  ::fivetran_sdk::v2::TestResponse response;

  auto status = service.Test(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);
}

TEST_CASE("Test endpoint block size validation fails when optional block size "
          "is not a valid number",
          "[integration][configtest]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::v2::TestRequest request;
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);
  request.set_name(CONFIG_TEST_NAME_CSV_BLOCK_SIZE);
  (*request.mutable_configuration())["motherduck_database"] =
      TEST_DATABASE_NAME;
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())[MD_PROP_CSV_BLOCK_SIZE] = "lizard";

  ::fivetran_sdk::v2::TestResponse response;

  auto status = service.Test(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);
  REQUIRE_FALSE(response.success());

  auto expected_message =
      "Test <test_csv_block_size> for database <" + TEST_DATABASE_NAME +
      "> failed: Maximum individual value size must be numeric if present";
  REQUIRE(response.failure() == expected_message);
}

template <typename T>
void define_test_table(T &request, const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
  auto col1 = request.mutable_table()->add_columns();
  col1->set_name("id");
  col1->set_type(::fivetran_sdk::v2::DataType::INT);
  col1->set_primary_key(true);

  auto col2 = request.mutable_table()->add_columns();
  col2->set_name("title");
  col2->set_type(::fivetran_sdk::v2::DataType::STRING);

  auto col3 = request.mutable_table()->add_columns();
  col3->set_name("magic_number");
  col3->set_type(::fivetran_sdk::v2::DataType::INT);

  auto col4 = request.mutable_table()->add_columns();
  col4->set_name("_fivetran_deleted");
  col4->set_type(::fivetran_sdk::v2::DataType::BOOLEAN);

  auto col5 = request.mutable_table()->add_columns();
  col5->set_name("_fivetran_synced");
  col5->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);
}

template <typename T>
void define_history_test_table(T &request, const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
  auto col1 = request.mutable_table()->add_columns();
  col1->set_name("id");
  col1->set_type(::fivetran_sdk::v2::DataType::INT);
  col1->set_primary_key(true);

  auto col2 = request.mutable_table()->add_columns();
  col2->set_name("title");
  col2->set_type(::fivetran_sdk::v2::DataType::STRING);

  auto col3 = request.mutable_table()->add_columns();
  col3->set_name("magic_number");
  col3->set_type(::fivetran_sdk::v2::DataType::INT);

  auto col4 = request.mutable_table()->add_columns();
  col4->set_name("_fivetran_deleted");
  col4->set_type(::fivetran_sdk::v2::DataType::BOOLEAN);

  auto col5 = request.mutable_table()->add_columns();
  col5->set_name("_fivetran_synced");
  col5->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);

  auto col6 = request.mutable_table()->add_columns();
  col6->set_name("_fivetran_active");
  col6->set_type(::fivetran_sdk::v2::DataType::BOOLEAN);

  auto col7 = request.mutable_table()->add_columns();
  col7->set_name("_fivetran_start");
  col7->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);
  col7->set_primary_key(true);

  auto col8 = request.mutable_table()->add_columns();
  col8->set_name("_fivetran_end");
  col8->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);
}

template <typename T>
void define_test_multikey_table(T &request, const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
  auto col1 = request.mutable_table()->add_columns();
  col1->set_name("id1");
  col1->set_type(::fivetran_sdk::v2::DataType::INT);
  col1->set_primary_key(true);

  auto col2 = request.mutable_table()->add_columns();
  col2->set_name("id2");
  col2->set_type(::fivetran_sdk::v2::DataType::INT);
  col2->set_primary_key(true);

  auto col3 = request.mutable_table()->add_columns();
  col3->set_name("text");
  col3->set_type(::fivetran_sdk::v2::DataType::STRING);

  auto col4 = request.mutable_table()->add_columns();
  col4->set_name("_fivetran_deleted");
  col4->set_type(::fivetran_sdk::v2::DataType::BOOLEAN);

  auto col5 = request.mutable_table()->add_columns();
  col5->set_name("_fivetran_synced");
  col5->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);
}

/* compression and encryption are off/none by default */
template <typename T>
void set_up_plain_write_request(T &request, const std::string &token,
                                const std::string db_name) {
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] = db_name;
}

std::unique_ptr<duckdb::Connection> get_test_connection(char *token) {
  duckdb::DBConfig config;
  config.SetOptionByName("motherduck_token", token);
  config.SetOptionByName("custom_user_agent", "fivetran-integration-test");
  duckdb::DuckDB db("md:" + TEST_DATABASE_NAME, &config);
  return std::make_unique<duckdb::Connection>(db);
}

TEST_CASE("WriteBatch", "[integration][write-batch]") {
  DestinationSdkImpl service;

  // Schema will be main
  const std::string table_name = "books" + std::to_string(Catch::rngSeed());

  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  {
    // Create Table
    ::fivetran_sdk::v2::CreateTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    define_test_table(request, table_name);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  auto con = get_test_connection(token);
  {
    // insert rows from encrypted / compressed file
    ::fivetran_sdk::v2::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_file_params()->set_encryption(
        ::fivetran_sdk::v2::Encryption::AES);
    request.mutable_file_params()->set_compression(
        ::fivetran_sdk::v2::Compression::ZSTD);
    define_test_table(request, table_name);
    const std::string filename = "books_batch_1_insert.csv.zst.aes";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    std::ifstream keyfile(filepath + ".key", std::ios::binary);
    char key[33];
    keyfile.read(key, 32);
    key[32] = 0;
    (*request.mutable_keys())[filepath] = key;

    request.add_replace_files(filepath);

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check inserted rows
    auto res = con->Query("SELECT id, title, magic_number FROM " + table_name +
                          " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);
    REQUIRE(res->GetValue(0, 0) == 1);
    REQUIRE(res->GetValue(1, 0) == "The Hitchhiker's Guide to the Galaxy");
    REQUIRE(res->GetValue(2, 0) == 42);

    REQUIRE(res->GetValue(0, 1) == 2);
    REQUIRE(res->GetValue(1, 1) == "The Lord of the Rings");
    REQUIRE(res->GetValue(2, 1) == 1);
  }

  {
    // upsert
    ::fivetran_sdk::v2::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    define_test_table(request, table_name);
    request.mutable_file_params()->set_null_string("magic-nullvalue");
    const std::string filename = "books_upsert.csv";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    request.add_replace_files(filepath);

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check after upsert
    auto res = con->Query("SELECT id, title, magic_number FROM " + table_name +
                          " ORDER BY id");
    REQUIRE_NO_FAIL(res);

    REQUIRE(res->RowCount() == 4);
    REQUIRE(res->GetValue(0, 0) == 1);
    REQUIRE(res->GetValue(1, 0) == "The Hitchhiker's Guide to the Galaxy");
    REQUIRE(res->GetValue(2, 0) == 42);

    REQUIRE(res->GetValue(0, 1) == 2);
    REQUIRE(res->GetValue(1, 1) == "The Two Towers"); // updated value
    REQUIRE(res->GetValue(2, 1) == 1);

    // new row
    REQUIRE(res->GetValue(0, 2) == 3);
    REQUIRE(res->GetValue(1, 2) == "The Hobbit");
    REQUIRE(res->GetValue(2, 2) == 14);

    // new row with null value
    REQUIRE(res->GetValue(0, 3) == 99);
    REQUIRE(res->GetValue(1, 3).IsNull() ==
            false); // a string with text "null" should not be null
    REQUIRE(res->GetValue(1, 3) == "null");
    REQUIRE(res->GetValue(2, 3).IsNull() == true);
  }

  {
    // delete
    ::fivetran_sdk::v2::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    define_test_table(request, table_name);
    const std::string filename = "books_delete.csv";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    request.add_delete_files(filepath);

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check after delete
    auto res = con->Query("SELECT id, title, magic_number FROM " + table_name +
                          " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 3);

    // row 1 got deleted
    REQUIRE(res->GetValue(0, 0) == 2);
    REQUIRE(res->GetValue(1, 0) == "The Two Towers");
    REQUIRE(res->GetValue(2, 0) == 1);

    REQUIRE(res->GetValue(0, 1) == 3);
    REQUIRE(res->GetValue(1, 1) == "The Hobbit");
    REQUIRE(res->GetValue(2, 1) == 14);

    REQUIRE(res->GetValue(0, 2) == 99);
    REQUIRE(res->GetValue(1, 2) == "null");
    REQUIRE(res->GetValue(2, 2).IsNull() == true);
  }

  {
    // update
    ::fivetran_sdk::v2::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_file_params()->set_unmodified_string(
        "unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
    request.mutable_file_params()->set_null_string("magic-nullvalue");
    define_test_table(request, table_name);
    const std::string filename = "books_update.csv";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    request.add_update_files(filepath);

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check after update
    auto res = con->Query("SELECT id, title, magic_number FROM " + table_name +
                          " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 3);

    REQUIRE(res->GetValue(0, 0) == 2);
    REQUIRE(res->GetValue(1, 0) == "The empire strikes back");
    REQUIRE(res->GetValue(2, 0) == 1);

    REQUIRE(res->GetValue(0, 1) == 3);
    REQUIRE(res->GetValue(1, 1) == "The Hobbit");
    REQUIRE(res->GetValue(2, 1) == 15); // updated value

    REQUIRE(res->GetValue(0, 2) == 99);
    REQUIRE(res->GetValue(1, 2).IsNull() == true);
    REQUIRE(res->GetValue(2, 2).IsNull() == false);
    REQUIRE(res->GetValue(2, 2) == 99);
  }

  {
    // truncate data before Jan 9 2024
    ::fivetran_sdk::v2::TruncateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);
    request.set_synced_column("_fivetran_synced");
    request.mutable_soft()->set_deleted_column("_fivetran_deleted");

    const auto cutoff_datetime = 1707436800; // 2024-02-09 0:0:0 GMT, trust me
    request.mutable_utc_delete_before()->set_seconds(cutoff_datetime);
    request.mutable_utc_delete_before()->set_nanos(0);
    ::fivetran_sdk::v2::TruncateResponse response;
    auto status = service.Truncate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check truncated table
    auto res = con->Query("SELECT title, id, magic_number FROM " + table_name +
                          " WHERE _fivetran_deleted = false ORDER BY id");
    REQUIRE_NO_FAIL(res);
    // the 1st row from books_update.csv that had 2024-02-08T23:59:59.999999999Z
    // timestamp got deleted
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0) == "The empire strikes back");
    REQUIRE(res->GetValue(1, 0) == 2);
    REQUIRE(res->GetValue(2, 0) == 1);
  }

  {
    // check the rows did not get physically deleted
    auto res = con->Query("SELECT title, id, magic_number FROM " + table_name +
                          " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 3);
  }

  {
    // truncate table does nothing if there is no utc_delete_before field set
    ::fivetran_sdk::v2::TruncateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);
    request.set_synced_column("_fivetran_synced");
    request.mutable_soft()->set_deleted_column("_fivetran_deleted");

    ::fivetran_sdk::v2::TruncateResponse response;
    auto status = service.Truncate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check truncated table is the same as before
    auto res = con->Query("SELECT title FROM " + table_name +
                          " WHERE _fivetran_deleted = false ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0) == "The empire strikes back");
  }

  {
    // check again that the rows did not get physically deleted
    auto res = con->Query("SELECT title, id, magic_number FROM " + table_name +
                          " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 3);
  }

  {
    // hard truncate all data (deleted_column not set in request)
    ::fivetran_sdk::v2::TruncateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);
    request.set_synced_column("_fivetran_synced");

    const auto cutoff_datetime =
        1893456000; // delete everything before 2030-01-01
    request.mutable_utc_delete_before()->set_seconds(cutoff_datetime);
    request.mutable_utc_delete_before()->set_nanos(0);
    ::fivetran_sdk::v2::TruncateResponse response;
    auto status = service.Truncate(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check the rows got physically deleted
    auto res = con->Query("SELECT title, id, magic_number FROM " + table_name +
                          " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 0);
  }
}

TEST_CASE("Table with multiple primary keys", "[integration][write-batch]") {
  DestinationSdkImpl service;

  const std::string table_name =
      "multikey_table" + std::to_string(Catch::rngSeed());
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  {
    // Create Table
    ::fivetran_sdk::v2::CreateTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    define_test_multikey_table(request, table_name);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe the created table
    ::fivetran_sdk::v2::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);

    {
      ::fivetran_sdk::v2::DescribeTableResponse response;
      auto status = service.DescribeTable(nullptr, &request, &response);
      REQUIRE_NO_FAIL(status);
      REQUIRE(response.table().columns().size() == 5);

      REQUIRE(response.table().columns(0).name() == "id1");
      REQUIRE(response.table().columns(1).name() == "id2");
      REQUIRE(response.table().columns(2).name() == "text");
      REQUIRE(response.table().columns(3).name() == "_fivetran_deleted");
      REQUIRE(response.table().columns(4).name() == "_fivetran_synced");
    }
  }

  // test connection needs to be created after table creation to avoid stale
  // catalog
  auto con = get_test_connection(token);
  {
    // insert rows
    ::fivetran_sdk::v2::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    define_test_multikey_table(request, table_name);
    const std::string filename = "multikey_table_upsert.csv";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    request.add_replace_files(filepath);

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check inserted rows
    auto res = con->Query("SELECT id1, id2, text FROM " + table_name +
                          " ORDER BY id1, id2");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 3);
    REQUIRE(res->GetValue(0, 0) == 1);
    REQUIRE(res->GetValue(1, 0) == 100);
    REQUIRE(res->GetValue(2, 0) == "first row");

    REQUIRE(res->GetValue(0, 1) == 2);
    REQUIRE(res->GetValue(1, 1) == 200);
    REQUIRE(res->GetValue(2, 1) == "second row");

    REQUIRE(res->GetValue(0, 2) == 3);
    REQUIRE(res->GetValue(1, 2) == 300);
    REQUIRE(res->GetValue(2, 2) == "third row");
  }

  {
    // update
    ::fivetran_sdk::v2::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_file_params()->set_unmodified_string(
        "magic-unmodified-value");
    request.mutable_file_params()->set_null_string("magic-nullvalue");
    define_test_multikey_table(request, table_name);
    const std::string filename = "multikey_table_update.csv";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    request.add_update_files(filepath);

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check after update, including a soft delete
    auto res = con->Query("SELECT id1, id2, text, _fivetran_deleted FROM " +
                          table_name + " ORDER BY id1, id2");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 3);
    REQUIRE(res->GetValue(0, 0) == 1);
    REQUIRE(res->GetValue(1, 0) == 100);
    REQUIRE(res->GetValue(2, 0) == "first row");
    REQUIRE(res->GetValue(3, 0) == false);

    REQUIRE(res->GetValue(0, 1) == 2);
    REQUIRE(res->GetValue(1, 1) == 200);
    REQUIRE(res->GetValue(2, 1) == "second row updated");
    REQUIRE(res->GetValue(3, 1) == false);

    REQUIRE(res->GetValue(0, 2) == 3);
    REQUIRE(res->GetValue(1, 2) == 300);
    REQUIRE(res->GetValue(2, 2) ==
            "third row soft deleted - but also this value updated");
    REQUIRE(res->GetValue(3, 2) == true);
  }

  {
    // delete
    ::fivetran_sdk::v2::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    define_test_multikey_table(request, table_name);
    const std::string filename = "multikey_table_delete.csv";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    request.add_delete_files(filepath);

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check after hard delete
    auto res = con->Query("SELECT id1, id2, text, _fivetran_deleted FROM " +
                          table_name + " ORDER BY id1, id2");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0) == 2);
    REQUIRE(res->GetValue(1, 0) == 200);
    REQUIRE(res->GetValue(2, 0) == "second row updated");
    REQUIRE(res->GetValue(3, 0) == false);
  }
}

TEST_CASE("CreateTable with JSON column", "[integration]") {
  DestinationSdkImpl service;

  const std::string table_name =
      "json_table" + std::to_string(Catch::rngSeed());
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  {
    // Create Table
    ::fivetran_sdk::v2::CreateTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_table()->set_name(table_name);
    auto col1 = request.mutable_table()->add_columns();
    col1->set_name("data");
    col1->set_type(::fivetran_sdk::v2::DataType::JSON);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe the created table
    ::fivetran_sdk::v2::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);

    {
      ::fivetran_sdk::v2::DescribeTableResponse response;
      auto status = service.DescribeTable(nullptr, &request, &response);
      REQUIRE_NO_FAIL(status);
      REQUIRE(response.table().columns().size() == 1);
      REQUIRE(response.table().columns(0).type() ==
              ::fivetran_sdk::v2::DataType::STRING);
    }
  }
}

template <typename T>
void make_book_table(T &request, const std::string &table_name) {

  request.mutable_table()->set_name(table_name);
  auto col0 = request.mutable_table()->add_columns();
  col0->set_name("id");
  col0->set_type(::fivetran_sdk::v2::DataType::INT);
  col0->set_primary_key(true);

  auto col1 = request.mutable_table()->add_columns();
  col1->set_name("text");
  col1->set_type(::fivetran_sdk::v2::DataType::STRING);
}

TEST_CASE("Table with large json row", "[integration][write-batch]") {
  DestinationSdkImpl service;

  const std::string table_name =
      "huge_book_" + std::to_string(Catch::rngSeed());
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  {
    // Create Table
    ::fivetran_sdk::v2::CreateTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    make_book_table(request, table_name);
    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  auto con = get_test_connection(token);
  {
    // fail when default block_size is used
    ::fivetran_sdk::v2::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;

    make_book_table(request, table_name);

    const std::string filename = "huge_books.csv";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    request.add_replace_files(filepath);

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_FALSE(status.ok());
    CHECK_THAT(status.error_message(),
               Catch::Matchers::ContainsSubstring(
                   "straddling object straddles two block boundaries"));
  }

  {
    // check no rows were inserted
    auto res = con->Query("SELECT count(*) FROM " + table_name);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0) == 0);
  }

  {
    // Empty string for the block size falls back to default value,
    // but sync fails due to default block size being too small.
    ::fivetran_sdk::v2::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    (*request.mutable_configuration())[MD_PROP_CSV_BLOCK_SIZE] = "";

    make_book_table(request, table_name);

    const std::string filename = "huge_books.csv";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    request.add_replace_files(filepath);

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_FALSE(status.ok());
    CHECK_THAT(status.error_message(),
               Catch::Matchers::ContainsSubstring(
                   "straddling object straddles two block boundaries"));
  }

  {
    // check no rows were inserted
    auto res = con->Query("SELECT count(*) FROM " + table_name);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0) == 0);
  }

  {
    // succeed when block_size is increased
    ::fivetran_sdk::v2::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    (*request.mutable_configuration())[MD_PROP_CSV_BLOCK_SIZE] = "2";

    make_book_table(request, table_name);

    const std::string filename = "huge_books.csv";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    request.add_replace_files(filepath);

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check one row was inserted
    auto res = con->Query("SELECT count(*) FROM " + table_name);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);
    REQUIRE(res->GetValue(0, 0) == 1);
  }
}

TEST_CASE("Truncate nonexistent table should succeed", "[integration]") {
  DestinationSdkImpl service;

  const std::string bad_table_name = "nonexistent";

  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  ::fivetran_sdk::v2::TruncateRequest request;
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] =
      TEST_DATABASE_NAME;
  request.set_schema_name("some_schema");
  request.set_table_name(bad_table_name);
  request.set_synced_column("_fivetran_synced");
  request.mutable_soft()->set_deleted_column("_fivetran_deleted");

  ::fivetran_sdk::v2::TruncateResponse response;

  std::stringstream buffer;
  std::streambuf *real_cout = std::cout.rdbuf(buffer.rdbuf());
  auto status = service.Truncate(nullptr, &request, &response);
  std::cout.rdbuf(real_cout);

  REQUIRE_NO_FAIL(status);
  REQUIRE_THAT(buffer.str(), Catch::Matchers::ContainsSubstring(
                                 "Table <nonexistent> not found in schema "
                                 "<some_schema>; not truncated"));
}

TEST_CASE("Truncate fails if synced_column is missing") {

  DestinationSdkImpl service;

  const std::string bad_table_name = "nonexistent";

  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  ::fivetran_sdk::v2::TruncateRequest request;
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] =
      TEST_DATABASE_NAME;
  request.set_table_name("some_table");

  ::fivetran_sdk::v2::TruncateResponse response;
  auto status = service.Truncate(nullptr, &request, &response);

  REQUIRE_FAIL(status, "Synced column is required");
}

TEST_CASE("reading inaccessible or nonexistent files fails") {
  DestinationSdkImpl service;

  const std::string bad_file_name = TEST_RESOURCES_DIR + "nonexistent.csv";
  ::fivetran_sdk::v2::WriteBatchRequest request;

  auto token = std::getenv("motherduck_token");
  REQUIRE(token);
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] =
      TEST_DATABASE_NAME;
  request.mutable_file_params()->set_encryption(
      ::fivetran_sdk::v2::Encryption::AES);
  request.mutable_file_params()->set_compression(
      ::fivetran_sdk::v2::Compression::ZSTD);
  define_test_table(request, "unused_table");

  request.add_replace_files(bad_file_name);
  (*request.mutable_keys())[bad_file_name] = "whatever";

  ::fivetran_sdk::v2::WriteBatchResponse response;
  auto status = service.WriteBatch(nullptr, &request, &response);
  const auto expected =
      "WriteBatch endpoint failed for schema <>, table <unused_table>:File <" +
      bad_file_name + "> is missing or inaccessible";
  REQUIRE_FAIL(status, expected);
}

TEST_CASE("Test all types with create and describe table") {

  DestinationSdkImpl service;

  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  const std::string table_name =
      "all_types_table" + std::to_string(Catch::rngSeed());

  {
    // Create Table
    ::fivetran_sdk::v2::CreateTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_table()->set_name(table_name);
    auto col1 = request.mutable_table()->add_columns();
    col1->set_name("col_string");
    col1->set_type(::fivetran_sdk::v2::DataType::STRING);
    col1->set_primary_key(true);
    auto col2 = request.mutable_table()->add_columns();
    col2->set_name("col_int");
    col2->set_type(::fivetran_sdk::v2::DataType::INT);
    col2->set_primary_key(true);
    auto col3 = request.mutable_table()->add_columns();
    col3->set_name("col_decimal");
    col3->set_type(::fivetran_sdk::v2::DataType::DECIMAL);
    col3->mutable_params()->mutable_decimal()->set_precision(20);
    col3->mutable_params()->mutable_decimal()->set_scale(11);

    auto col4 = request.mutable_table()->add_columns();
    col4->set_name("col_utc_datetime");
    col4->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);
    auto col5 = request.mutable_table()->add_columns();
    col5->set_name("col_naive_datetime");
    col5->set_type(::fivetran_sdk::v2::DataType::NAIVE_DATETIME);
    auto col6 = request.mutable_table()->add_columns();
    col6->set_name("col_naive_date");
    col6->set_type(::fivetran_sdk::v2::DataType::NAIVE_DATE);

    auto col7 = request.mutable_table()->add_columns();
    col7->set_name("col_boolean");
    col7->set_type(::fivetran_sdk::v2::DataType::BOOLEAN);
    auto col8 = request.mutable_table()->add_columns();
    col8->set_name("col_short");
    col8->set_type(::fivetran_sdk::v2::DataType::SHORT);
    auto col9 = request.mutable_table()->add_columns();
    col9->set_name("col_long");
    col9->set_type(::fivetran_sdk::v2::DataType::LONG);
    auto col10 = request.mutable_table()->add_columns();
    col10->set_name("col_float");
    col10->set_type(::fivetran_sdk::v2::DataType::FLOAT);
    auto col11 = request.mutable_table()->add_columns();
    col11->set_name("col_double");
    col11->set_type(::fivetran_sdk::v2::DataType::DOUBLE);
    auto col12 = request.mutable_table()->add_columns();
    col12->set_name("col_binary");
    col12->set_type(::fivetran_sdk::v2::DataType::BINARY);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe table
    ::fivetran_sdk::v2::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);

    ::fivetran_sdk::v2::DescribeTableResponse response;
    auto status = service.DescribeTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(!response.not_found());

    REQUIRE(response.table().name() == table_name);
    REQUIRE(response.table().columns_size() == 12);

    REQUIRE(response.table().columns(0).name() == "col_string");
    REQUIRE(response.table().columns(0).type() ==
            ::fivetran_sdk::v2::DataType::STRING);
    REQUIRE(response.table().columns(0).primary_key());
    REQUIRE(response.table().columns(1).name() == "col_int");
    REQUIRE(response.table().columns(1).type() ==
            ::fivetran_sdk::v2::DataType::INT);
    REQUIRE(response.table().columns(1).primary_key());

    REQUIRE(response.table().columns(2).name() == "col_decimal");
    REQUIRE(response.table().columns(2).type() ==
            ::fivetran_sdk::v2::DataType::DECIMAL);
    REQUIRE(response.table().columns(2).has_params());
    REQUIRE(response.table().columns(2).params().has_decimal());

    REQUIRE(response.table().columns(2).params().decimal().scale() == 11);
    REQUIRE(response.table().columns(2).params().decimal().precision() == 20);
    REQUIRE_FALSE(response.table().columns(2).primary_key());

    REQUIRE(response.table().columns(3).name() == "col_utc_datetime");
    REQUIRE(response.table().columns(3).type() ==
            ::fivetran_sdk::v2::DataType::UTC_DATETIME);
    REQUIRE_FALSE(response.table().columns(3).primary_key());
    REQUIRE(response.table().columns(4).name() == "col_naive_datetime");
    REQUIRE(response.table().columns(4).type() ==
            ::fivetran_sdk::v2::DataType::NAIVE_DATETIME);
    REQUIRE_FALSE(response.table().columns(4).primary_key());
    REQUIRE(response.table().columns(5).name() == "col_naive_date");
    REQUIRE(response.table().columns(5).type() ==
            ::fivetran_sdk::v2::DataType::NAIVE_DATE);
    REQUIRE_FALSE(response.table().columns(5).primary_key());

    REQUIRE(response.table().columns(6).name() == "col_boolean");
    REQUIRE(response.table().columns(6).type() ==
            ::fivetran_sdk::v2::DataType::BOOLEAN);
    REQUIRE_FALSE(response.table().columns(6).primary_key());
    REQUIRE(response.table().columns(7).name() == "col_short");
    REQUIRE(response.table().columns(7).type() ==
            ::fivetran_sdk::v2::DataType::SHORT);
    REQUIRE_FALSE(response.table().columns(7).primary_key());
    REQUIRE(response.table().columns(8).name() == "col_long");
    REQUIRE(response.table().columns(8).type() ==
            ::fivetran_sdk::v2::DataType::LONG);
    REQUIRE_FALSE(response.table().columns(8).primary_key());
    REQUIRE(response.table().columns(9).name() == "col_float");
    REQUIRE(response.table().columns(9).type() ==
            ::fivetran_sdk::v2::DataType::FLOAT);
    REQUIRE_FALSE(response.table().columns(9).primary_key());
    REQUIRE(response.table().columns(10).name() == "col_double");
    REQUIRE(response.table().columns(10).type() ==
            ::fivetran_sdk::v2::DataType::DOUBLE);
    REQUIRE_FALSE(response.table().columns(10).primary_key());
    REQUIRE(response.table().columns(11).name() == "col_binary");
    REQUIRE(response.table().columns(11).type() ==
            ::fivetran_sdk::v2::DataType::BINARY);
    REQUIRE_FALSE(response.table().columns(11).primary_key());
  }
}

template <typename T>
void add_config(T &request, const std::string &token,
                const std::string &database, const std::string &table) {
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] = database;
  request.mutable_table()->set_name(table);
}

template <typename T>
void add_col(T &request, const std::string &name,
             ::fivetran_sdk::v2::DataType type, bool is_primary_key) {
  auto col = request.mutable_table()->add_columns();
  col->set_name(name);
  col->set_type(type);
  col->set_primary_key(is_primary_key);
}

TEST_CASE("AlterTable with constraints", "[integration]") {
  DestinationSdkImpl service;

  const std::string table_name =
      "some_table" + std::to_string(Catch::rngSeed());
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  auto con = get_test_connection(token);

  {
    // Create Table
    ::fivetran_sdk::v2::CreateTableRequest request;
    add_config(request, token, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
    add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, false);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Alter Table to add a new primary key to an empty table
    ::fivetran_sdk::v2::AlterTableRequest request;

    add_config(request, token, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
    add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, false);
    add_col(request, "id_new", ::fivetran_sdk::v2::DataType::INT, true);

    ::fivetran_sdk::v2::AlterTableResponse response;
    auto status = service.AlterTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe the altered table
    ::fivetran_sdk::v2::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);

    ::fivetran_sdk::v2::DescribeTableResponse response;
    auto status = service.DescribeTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(!response.not_found());

    REQUIRE(response.table().name() == table_name);
    REQUIRE(response.table().columns_size() == 3);
    REQUIRE(response.table().columns(0).name() == "id");
    REQUIRE(response.table().columns(0).type() ==
            ::fivetran_sdk::v2::DataType::STRING);
    REQUIRE(response.table().columns(0).primary_key());

    REQUIRE(response.table().columns(1).name() == "name");
    REQUIRE(response.table().columns(1).type() ==
            ::fivetran_sdk::v2::DataType::STRING);
    REQUIRE_FALSE(response.table().columns(1).primary_key());

    REQUIRE(response.table().columns(2).name() == "id_new");
    REQUIRE(response.table().columns(2).type() ==
            ::fivetran_sdk::v2::DataType::INT);
    REQUIRE(response.table().columns(2).primary_key());
  }

  {
    // Insert some test data to validate after primary key modification
    auto res = con->Query("INSERT INTO " + table_name +
                          "(id, name, id_new) VALUES (1, 'one', 101)");
    REQUIRE_NO_FAIL(res);
    auto res2 = con->Query("INSERT INTO " + table_name +
                           "(id, name, id_new) VALUES (2, 'two', 102)");
    REQUIRE_NO_FAIL(res2);
  }

  {
    // Alter Table to make an existing (unique valued) column into a primary key
    ::fivetran_sdk::v2::AlterTableRequest request;

    add_config(request, token, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
    // turn existing column into a primary key
    add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, true);
    add_col(request, "id_new", ::fivetran_sdk::v2::DataType::INT, true);

    ::fivetran_sdk::v2::AlterTableResponse response;
    auto status = service.AlterTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe the altered table
    ::fivetran_sdk::v2::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);

    ::fivetran_sdk::v2::DescribeTableResponse response;
    auto status = service.DescribeTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(!response.not_found());

    REQUIRE(response.table().name() == table_name);
    REQUIRE(response.table().columns_size() == 3);
    REQUIRE(response.table().columns(0).name() == "id");
    REQUIRE(response.table().columns(0).type() ==
            ::fivetran_sdk::v2::DataType::STRING);
    REQUIRE(response.table().columns(0).primary_key());

    REQUIRE(response.table().columns(1).name() == "name");
    REQUIRE(response.table().columns(1).type() ==
            ::fivetran_sdk::v2::DataType::STRING);
    REQUIRE(response.table().columns(1).primary_key());

    REQUIRE(response.table().columns(2).name() == "id_new");
    REQUIRE(response.table().columns(2).type() ==
            ::fivetran_sdk::v2::DataType::INT);
    REQUIRE(response.table().columns(2).primary_key());
  }

  {
    // Make sure the data is still correct after recreating the table
    auto res = con->Query("SELECT id, name, id_new FROM " + table_name);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);
    REQUIRE(res->GetValue(0, 0) == "1");
    REQUIRE(res->GetValue(1, 0) == "one");
    REQUIRE(res->GetValue(2, 0) == 101);
    REQUIRE(res->GetValue(0, 1) == "2");
    REQUIRE(res->GetValue(1, 1) == "two");
    REQUIRE(res->GetValue(2, 1) == 102);
  }

  {
    // Alter Table to drop a primary key column -- should be a no-op as dropping columns is not allowed
    ::fivetran_sdk::v2::AlterTableRequest request;

    add_config(request, token, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
    add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, true);

    ::fivetran_sdk::v2::AlterTableResponse response;
    auto status = service.AlterTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe the altered table
    ::fivetran_sdk::v2::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);

    ::fivetran_sdk::v2::DescribeTableResponse response;
    auto status = service.DescribeTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(!response.not_found());

    REQUIRE(response.table().name() == table_name);
    REQUIRE(response.table().columns_size() == 3);
    REQUIRE(response.table().columns(0).name() == "id");
    REQUIRE(response.table().columns(0).type() ==
            ::fivetran_sdk::v2::DataType::STRING);
    REQUIRE(response.table().columns(0).primary_key());

    REQUIRE(response.table().columns(1).name() == "name");
    REQUIRE(response.table().columns(1).type() ==
            ::fivetran_sdk::v2::DataType::STRING);
    REQUIRE(response.table().columns(1).primary_key());

		REQUIRE(response.table().columns(2).name() == "id_new");
		REQUIRE(response.table().columns(2).type() ==
						::fivetran_sdk::v2::DataType::INT);
		REQUIRE(response.table().columns(2).primary_key());
  }

  {
    // Make sure the data is still correct after recreating the table
    auto res = con->Query("SELECT id, name FROM " + table_name);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);
    REQUIRE(res->GetValue(0, 0) == "1");
    REQUIRE(res->GetValue(1, 0) == "one");
    REQUIRE(res->GetValue(0, 1) == "2");
    REQUIRE(res->GetValue(1, 1) == "two");
  }

  {
    // Alter Table to add new primary key columns with correct defaults
    ::fivetran_sdk::v2::AlterTableRequest request;

    add_config(request, token, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
    add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, true);

    add_col(request, "id_int", ::fivetran_sdk::v2::DataType::INT, true);
    add_col(request, "id_varchar", ::fivetran_sdk::v2::DataType::STRING, true);
    add_col(request, "id_date", ::fivetran_sdk::v2::DataType::NAIVE_DATE, true);
    add_col(request, "id_float", ::fivetran_sdk::v2::DataType::FLOAT, true);

    ::fivetran_sdk::v2::AlterTableResponse response;
    auto status = service.AlterTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Make sure the defaults are set correctly
    auto res = con->Query("SELECT * FROM " + table_name);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);
    REQUIRE(res->GetValue(0, 0) == "1");
    REQUIRE(res->GetValue(1, 0) == "one");
    REQUIRE(res->GetValue(2, 0) == 0);
    REQUIRE(res->GetValue(3, 0) == "");
    REQUIRE(res->GetValue(4, 0) == "1970-01-01");
    REQUIRE(res->GetValue(5, 0) == 0.0);
  }

  {
    // Alter Table make a column no longer be a primary key AND change its type
    ::fivetran_sdk::v2::AlterTableRequest request;

    add_config(request, token, TEST_DATABASE_NAME, table_name);
    add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
    add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, true);

    add_col(request, "id_int", ::fivetran_sdk::v2::DataType::LONG, false);
    add_col(request, "id_varchar", ::fivetran_sdk::v2::DataType::STRING, true);
    add_col(request, "id_date", ::fivetran_sdk::v2::DataType::NAIVE_DATE, true);
    add_col(request, "id_float", ::fivetran_sdk::v2::DataType::FLOAT, true);

    ::fivetran_sdk::v2::AlterTableResponse response;
    auto status = service.AlterTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe the altered table
    ::fivetran_sdk::v2::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);

    ::fivetran_sdk::v2::DescribeTableResponse response;
    auto status = service.DescribeTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(!response.not_found());

    REQUIRE(response.table().name() == table_name);
    REQUIRE(response.table().columns_size() == 6);
    REQUIRE(response.table().columns(0).name() == "id");
    REQUIRE(response.table().columns(0).type() ==
            ::fivetran_sdk::v2::DataType::STRING);
    REQUIRE(response.table().columns(0).primary_key());

    REQUIRE(response.table().columns(1).name() == "name");
    REQUIRE(response.table().columns(1).type() ==
            ::fivetran_sdk::v2::DataType::STRING);
    REQUIRE(response.table().columns(1).primary_key());

    REQUIRE(response.table().columns(2).name() == "id_int");
    REQUIRE(response.table().columns(2).type() ==
            ::fivetran_sdk::v2::DataType::LONG); // this type got updated
    REQUIRE_FALSE(response.table().columns(2).primary_key());

    REQUIRE(response.table().columns(3).name() == "id_varchar");
    REQUIRE(response.table().columns(3).type() ==
            ::fivetran_sdk::v2::DataType::STRING);
    REQUIRE(response.table().columns(3).primary_key());

    REQUIRE(response.table().columns(4).name() == "id_date");
    REQUIRE(response.table().columns(4).type() ==
            ::fivetran_sdk::v2::DataType::NAIVE_DATE);
    REQUIRE(response.table().columns(4).primary_key());

    REQUIRE(response.table().columns(5).name() == "id_float");
    REQUIRE(response.table().columns(5).type() ==
            ::fivetran_sdk::v2::DataType::FLOAT);
    REQUIRE(response.table().columns(5).primary_key());
  }

  {
    // Make sure the defaults are set correctly
    auto res = con->Query("SELECT * FROM " + table_name);
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);
    REQUIRE(res->GetValue(0, 0) == "1");
    REQUIRE(res->GetValue(1, 0) == "one");
    REQUIRE(res->GetValue(2, 0) == 0);
    REQUIRE(res->GetValue(3, 0) == "");
    REQUIRE(res->GetValue(4, 0) == "1970-01-01");
    REQUIRE(res->GetValue(5, 0) == 0.0);
  }
}

TEST_CASE("Invalid truncate with nonexisting delete column",
          "[integration][current]") {
  DestinationSdkImpl service;

  const std::string table_name =
      "empty_table" + std::to_string(Catch::rngSeed());
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  {
    // Create Table that is missing the _fivetran_deleted column
    ::fivetran_sdk::v2::CreateTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.mutable_table()->set_name(table_name);
    auto col1 = request.mutable_table()->add_columns();
    col1->set_name("something");
    col1->set_type(::fivetran_sdk::v2::DataType::STRING);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Attempt to truncate the table using a nonexisting _fivetran_deleted
    // column
    ::fivetran_sdk::v2::TruncateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] =
        TEST_DATABASE_NAME;
    request.set_table_name(table_name);
    request.set_synced_column(
        "_fivetran_synced"); // also does not exist although that does not
                             // matter
    request.mutable_soft()->set_deleted_column("_fivetran_deleted");

    const auto cutoff_datetime = 1707436800; // 2024-02-09 0:0:0 GMT, trust me
    request.mutable_utc_delete_before()->set_seconds(cutoff_datetime);
    request.mutable_utc_delete_before()->set_nanos(0);
    ::fivetran_sdk::v2::TruncateResponse response;
    auto status = service.Truncate(nullptr, &request, &response);
    REQUIRE_FALSE(status.ok());
    CHECK_THAT(
        status.error_message(),
        Catch::Matchers::ContainsSubstring(
            "Referenced column \"_fivetran_synced\" not found in FROM clause"));
  }
}

TEST_CASE("Capabilities", "[integration]") {
  DestinationSdkImpl service;
  ::fivetran_sdk::v2::CapabilitiesRequest request;
  ::fivetran_sdk::v2::CapabilitiesResponse response;

  auto status = service.Capabilities(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);

  REQUIRE(response.batch_file_format() == ::fivetran_sdk::v2::CSV);
}

TEST_CASE("WriteBatchHistory with update files", "[integration][write-batch]") {
  DestinationSdkImpl service;

  // Schema will be main
  const std::string table_name = "books" + std::to_string(Catch::rngSeed());

  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  {
    // Create Table
    ::fivetran_sdk::v2::CreateTableRequest request;
    set_up_plain_write_request(request, token, TEST_DATABASE_NAME);
    define_history_test_table(request, table_name);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // upsert some data, so that delete-earliest has something to delete
    ::fivetran_sdk::v2::WriteBatchRequest request;
    set_up_plain_write_request(request, token, TEST_DATABASE_NAME);
    define_history_test_table(request, table_name);

    request.mutable_file_params()->set_null_string("magic-nullvalue");
    request.add_replace_files(TEST_RESOURCES_DIR +
                              "books_history_upsert_regular_write.csv");

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // a WriteHistoryBatchRequest with only the earliest files provided.
    // (not a realistic scenario, but useful to isolate changes and test
    // idempotence in the next section)
    ::fivetran_sdk::v2::WriteHistoryBatchRequest request;
    set_up_plain_write_request(request, token, TEST_DATABASE_NAME);
    define_history_test_table(request, table_name);

    request.add_earliest_start_files(TEST_RESOURCES_DIR +
                                     "books_history_earliest.csv");

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteHistoryBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  auto con = get_test_connection(token);
  {
    // check that id=2 ("The Two Towers") got deleted because it's newer than
    // the date in books_history_earliest.csv
    auto res = con->Query(
        "SELECT id, title, magic_number, _fivetran_deleted, _fivetran_synced, "
        "_fivetran_active, _fivetran_start, _fivetran_end"
        " FROM " +
        table_name + " ORDER BY id");
    REQUIRE_NO_FAIL(res);

    REQUIRE(res->RowCount() == 3);

    // old outdated record, not affected at all
    REQUIRE(res->GetValue(0, 0) == 3);
    REQUIRE(res->GetValue(1, 0) == "The old Hobbit");
    REQUIRE(res->GetValue(2, 0) == 100);
    REQUIRE(res->GetValue(3, 0) == false);                           // deleted
    REQUIRE(res->GetValue(4, 0) == "2023-01-09 04:10:19.156057+00"); // synced
    REQUIRE(res->GetValue(5, 0) == false); // no longer active
    REQUIRE(res->GetValue(6, 0) ==
            "2023-01-09 04:10:19.156057+00"); // _fivetran_start
    REQUIRE(res->GetValue(7, 0) ==
            "2024-01-09 04:10:19.155957+00"); // _fivetran_end

    // latest record as of right before this WriteHistoryBatch; should get
    // deactivated
    REQUIRE(res->GetValue(0, 1) == 3);
    REQUIRE(res->GetValue(1, 1) == "The Hobbit");
    REQUIRE(res->GetValue(2, 1) == 14);
    REQUIRE(res->GetValue(3, 1) == false);                           // deleted
    REQUIRE(res->GetValue(4, 1) == "2024-01-09 04:10:19.156057+00"); // synced
    REQUIRE(res->GetValue(5, 1) == false); // no longer active
    REQUIRE(res->GetValue(6, 1) ==
            "2024-01-09 04:10:19.156057+00"); // _fivetran_start
    REQUIRE(res->GetValue(7, 1) ==
            "2025-01-01 20:56:59.999+00"); // _fivetran_end updated to 1ms
                                           // before the earliest

    // active record that's not part of the batch; not affected at all
    REQUIRE(res->GetValue(0, 2) == 99);
    REQUIRE(res->GetValue(5, 2) == true); // this primary key was not in the
                                          // incoming batch, so not deactivated
    REQUIRE(res->GetValue(6, 2) ==
            "2025-01-09 04:10:19.156057+00"); // _fivetran_start, no change
    REQUIRE(res->GetValue(7, 2) ==
            "9999-01-09 04:10:19.156057+00"); // _fivetran_end, no change
  }

  {
    // a WriteHistoryBatchRequest with the same earliest files as before, plus
    // update files
    ::fivetran_sdk::v2::WriteHistoryBatchRequest request;
    set_up_plain_write_request(request, token, TEST_DATABASE_NAME);
    // TBD: check what happens when unmodified string is not set - it seems to
    // be blank but shoudl it fail instead?
    request.mutable_file_params()->set_unmodified_string(
        "unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
    request.mutable_file_params()->set_null_string("magic-nullvalue");

    define_history_test_table(request, table_name);

    request.add_earliest_start_files(TEST_RESOURCES_DIR +
                                     "books_history_earliest.csv");
    request.add_update_files(TEST_RESOURCES_DIR + "books_history_update.csv");

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteHistoryBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check that id=2 ("The Two Towers") got deleted because it's newer than
    // the date in books_history_earliest.csv
    auto res = con->Query(
        "SELECT id, title, magic_number, _fivetran_deleted, _fivetran_synced, "
        "_fivetran_active, _fivetran_start, _fivetran_end"
        " FROM " +
        table_name + " ORDER BY id, _fivetran_start");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 5);

    // new record for id=2
    REQUIRE(res->GetValue(0, 0) == 2);
    REQUIRE(res->GetValue(1, 0) == "The empire strikes back");
    REQUIRE(
        res->GetValue(2, 0)
            .IsNull()); // there was no previous record, so no previous value
    REQUIRE(res->GetValue(3, 0) == false);                    // deleted
    REQUIRE(res->GetValue(4, 0) == "2025-02-08 12:00:00+00"); // synced
    REQUIRE(res->GetValue(5, 0) == true); // active, as per file
    REQUIRE(res->GetValue(6, 0) == "2025-02-08 12:00:00+00"); // _fivetran_start
    REQUIRE(res->GetValue(7, 0) == "9999-02-08 12:00:00+00"); // _fivetran_end

    // old outdated record, not affected at all
    REQUIRE(res->GetValue(0, 1) == 3);
    REQUIRE(res->GetValue(1, 1) == "The old Hobbit");
    REQUIRE(res->GetValue(2, 1) == 100);
    REQUIRE(res->GetValue(3, 1) == false);                           // deleted
    REQUIRE(res->GetValue(4, 1) == "2023-01-09 04:10:19.156057+00"); // synced
    REQUIRE(res->GetValue(5, 1) == false); // no longer active
    REQUIRE(res->GetValue(6, 1) ==
            "2023-01-09 04:10:19.156057+00"); // _fivetran_start
    REQUIRE(res->GetValue(7, 1) ==
            "2024-01-09 04:10:19.155957+00"); // _fivetran_end

    // no change in the historical record from the last check
    REQUIRE(res->GetValue(0, 2) == 3);
    REQUIRE(res->GetValue(1, 2) == "The Hobbit");
    REQUIRE(res->GetValue(2, 2) == 14);
    REQUIRE(res->GetValue(3, 2) == false);                           // deleted
    REQUIRE(res->GetValue(4, 2) == "2024-01-09 04:10:19.156057+00"); // synced
    REQUIRE(res->GetValue(5, 2) == false); // no longer active
    REQUIRE(res->GetValue(6, 2) ==
            "2024-01-09 04:10:19.156057+00"); // _fivetran_start
    REQUIRE(res->GetValue(7, 2) ==
            "2025-01-01 20:56:59.999+00"); // _fivetran_end updated to 1ms
                                           // before the earliest

    // new version of an existing record
    REQUIRE(res->GetValue(0, 3) == 3);
    // unmodified value came through from previous version;
    // did not accidentally pick up "The old Hobbit" from an older record
    REQUIRE(res->GetValue(1, 3) == "The Hobbit");
    REQUIRE(res->GetValue(2, 3) == 123);
    REQUIRE(res->GetValue(3, 3) == false);                    // deleted
    REQUIRE(res->GetValue(4, 3) == "2025-02-08 12:00:00+00"); // synced
    REQUIRE(res->GetValue(5, 3) == true); // new version active, per file
    REQUIRE(res->GetValue(6, 3) == "2025-02-08 12:00:00+00"); // _fivetran_start
    REQUIRE(res->GetValue(7, 3) ==
            "9999-02-08 12:00:00+00"); // _fivetran_end updated to 1ms before
                                       // the earliest

    // no change in the historical record from the last check
    REQUIRE(res->GetValue(0, 4) == 99);
    REQUIRE(res->GetValue(5, 4) == true); // this primary key was not in the
                                          // incoming batch, so not deactivated
    REQUIRE(res->GetValue(6, 4) ==
            "2025-01-09 04:10:19.156057+00"); // _fivetran_start, no change
    REQUIRE(res->GetValue(7, 4) ==
            "9999-01-09 04:10:19.156057+00"); // _fivetran_end, no change
  }
}

TEST_CASE("WriteBatchHistory upsert and delete", "[integration][write-batch]") {
  DestinationSdkImpl service;

  // Schema will be main
  const std::string table_name = "books" + std::to_string(Catch::rngSeed());

  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  {
    // Create Table
    ::fivetran_sdk::v2::CreateTableRequest request;
    set_up_plain_write_request(request, token, TEST_DATABASE_NAME);
    define_history_test_table(request, table_name);

    ::fivetran_sdk::v2::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // history write with the earliest file (that does not affect anything
    // because there is no data), plus upsert file
    ::fivetran_sdk::v2::WriteHistoryBatchRequest request;
    set_up_plain_write_request(request, token, TEST_DATABASE_NAME);
    request.mutable_file_params()->set_unmodified_string(
        "unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
    request.mutable_file_params()->set_null_string("magic-nullvalue");

    define_history_test_table(request, table_name);

    request.add_earliest_start_files(TEST_RESOURCES_DIR +
                                     "books_history_earliest.csv");
    request.add_update_files(TEST_RESOURCES_DIR + "books_history_upsert.csv");

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteHistoryBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  auto con = get_test_connection(token);
  {
    auto res = con->Query(
        "SELECT id, title, magic_number, _fivetran_deleted, _fivetran_synced, "
        "_fivetran_active, _fivetran_start, _fivetran_end"
        " FROM " +
        table_name + " ORDER BY id, _fivetran_start");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);

    // record inserted as is
    REQUIRE(res->GetValue(0, 0) == 3);
    REQUIRE(res->GetValue(1, 0) == "The Hobbit");
    REQUIRE(res->GetValue(2, 0) == 14);
    REQUIRE(res->GetValue(3, 0) == false);                           // deleted
    REQUIRE(res->GetValue(4, 0) == "2024-01-09 04:10:19.156057+00"); // synced
    REQUIRE(res->GetValue(5, 0) == true); // active, per file
    REQUIRE(res->GetValue(6, 0) ==
            "2024-01-09 04:10:19.156057+00"); // _fivetran_start
    REQUIRE(res->GetValue(7, 0) ==
            "9999-01-09 04:10:19.156057+00"); // _fivetran_end
  }

  {
    // history write with just the delete file (for testing; normally there
    // would also be the earliest start file)
    ::fivetran_sdk::v2::WriteHistoryBatchRequest request;
    set_up_plain_write_request(request, token, TEST_DATABASE_NAME);
    request.mutable_file_params()->set_unmodified_string(
        "unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
    request.mutable_file_params()->set_null_string("magic-nullvalue");

    define_history_test_table(request, table_name);

    request.add_delete_files(TEST_RESOURCES_DIR + "books_history_delete.csv");

    ::fivetran_sdk::v2::WriteBatchResponse response;
    auto status = service.WriteHistoryBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check that id=2 ("The Two Towers") got deleted because it's newer than
    // the date in books_history_earliest.csv
    auto res = con->Query(
        "SELECT id, title, magic_number, _fivetran_deleted, _fivetran_synced, "
        "_fivetran_active, _fivetran_start, _fivetran_end"
        " FROM " +
        table_name + " ORDER BY id, _fivetran_start");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 1);

    // record inserted as is
    REQUIRE(res->GetValue(0, 0) == 3);
    REQUIRE(res->GetValue(1, 0) == "The Hobbit");
    REQUIRE(res->GetValue(2, 0) == 14);
    REQUIRE(res->GetValue(3, 0) == false);                           // deleted
    REQUIRE(res->GetValue(4, 0) == "2024-01-09 04:10:19.156057+00"); // synced
    REQUIRE(res->GetValue(5, 0) == false); // no longer active
    REQUIRE(res->GetValue(6, 0) ==
            "2024-01-09 04:10:19.156057+00"); // _fivetran_start
    REQUIRE(res->GetValue(7, 0) ==
            "2025-03-09 04:10:19.156057+00"); // _fivetran_end updated per
                                              // delete file
  }
}


TEST_CASE("AlterTable must not drop columns", "[integration]") {
	DestinationSdkImpl service;

	const std::string table_name =
			"some_table" + std::to_string(Catch::rngSeed());
	auto token = std::getenv("motherduck_token");
	REQUIRE(token);

	auto con = get_test_connection(token);

	{
		// Create Table
		::fivetran_sdk::v2::CreateTableRequest request;
		add_config(request, token, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, false);

		::fivetran_sdk::v2::CreateTableResponse response;
		auto status = service.CreateTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	{
		// Alter Table to drop a regular column -- no-op because columns must not be deleted
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, token, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, true);
		// the second column is missing, but it should be retained

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	auto verifyTableStructure = [&]() {
			// Describe the altered table
			::fivetran_sdk::v2::DescribeTableRequest request;
			(*request.mutable_configuration())["motherduck_token"] = token;
			(*request.mutable_configuration())["motherduck_database"] =
					TEST_DATABASE_NAME;
			request.set_table_name(table_name);

			::fivetran_sdk::v2::DescribeTableResponse response;
			auto status = service.DescribeTable(nullptr, &request, &response);
			REQUIRE_NO_FAIL(status);
			REQUIRE(!response.not_found());

			REQUIRE(response.table().name() == table_name);
			REQUIRE(response.table().columns_size() == 2);
			REQUIRE(response.table().columns(0).name() == "id");
			REQUIRE(response.table().columns(0).type() ==
							::fivetran_sdk::v2::DataType::STRING);
			REQUIRE(response.table().columns(0).primary_key());

			REQUIRE(response.table().columns(1).name() == "name");
			REQUIRE(response.table().columns(1).type() ==
							::fivetran_sdk::v2::DataType::STRING);
			REQUIRE_FALSE(response.table().columns(1).primary_key());
	};

	verifyTableStructure();

	{
		// Alter Table to drop a primary key column -- no-op because columns must not be deleted
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, token, TEST_DATABASE_NAME, table_name);
		// the first column is missing, but it should be retained
		add_col(request, "name", ::fivetran_sdk::v2::DataType::STRING, false);

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	verifyTableStructure();

	{
		// Alter Table to change the type on a primary key column and drop the regular column
		// Still no-op but needs a separate test because changing primary key status results in table recreation,
		// so could accidentally cause a column to be dropped
		::fivetran_sdk::v2::AlterTableRequest request;

		add_config(request, token, TEST_DATABASE_NAME, table_name);
		add_col(request, "id", ::fivetran_sdk::v2::DataType::STRING, false);	// primary key to regular column
		// the second column is missing, but it should be retained

		::fivetran_sdk::v2::AlterTableResponse response;
		auto status = service.AlterTable(nullptr, &request, &response);
		REQUIRE_NO_FAIL(status);
	}

	verifyTableStructure();

}