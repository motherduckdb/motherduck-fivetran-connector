#include "duckdb.hpp"
#include "motherduck_destination_server.hpp"
#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <fstream>

#define STRING(x) #x
#define XSTRING(s) STRING(s)
const std::string TEST_RESOURCES_DIR = XSTRING(TEST_RESOURCES_LOCATION);

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

bool IS_FAIL(const grpc::Status &status, const std::string &expected_error) {
  if (!status.ok() && status.error_message() != expected_error) {
    fprintf(stderr, "Query failed with unexpected message: %s\n",
            status.error_message().c_str());
  }
  return !status.ok();
}
#define REQUIRE_NO_FAIL(result) REQUIRE(NO_FAIL((result)))
#define REQUIRE_FAIL(result, expected_error)                                   \
  REQUIRE(IS_FAIL(result, expected_error))

TEST_CASE("ConfigurationForm", "[integration]") {
  DestinationSdkImpl service;

  auto request = ::fivetran_sdk::ConfigurationFormRequest().New();
  auto response = ::fivetran_sdk::ConfigurationFormResponse().New();

  auto status = service.ConfigurationForm(nullptr, request, response);
  REQUIRE_NO_FAIL(status);

  REQUIRE(response->fields_size() == 2);
  REQUIRE(response->fields(0).name() == "motherduck_token");
  REQUIRE(response->fields(1).name() == "motherduck_database");
  REQUIRE(response->tests_size() == 1);
  REQUIRE(response->tests(0).name() == CONFIG_TEST_NAME_AUTHENTICATE);
  REQUIRE(response->tests(0).label() == "Test Authentication");
}

TEST_CASE("DescribeTable fails when database missing", "[integration]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::DescribeTableRequest request;
  (*request.mutable_configuration())["motherduck_token"] = "12345";

  ::fivetran_sdk::DescribeTableResponse response;

  auto status = service.DescribeTable(nullptr, &request, &response);
  REQUIRE_FAIL(status, "Missing property motherduck_database");
}

TEST_CASE("DescribeTable on nonexistent table", "[integration]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::DescribeTableRequest request;
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
  request.set_table_name("nonexistent_table");
  ::fivetran_sdk::DescribeTableResponse response;

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
    ::fivetran_sdk::CreateTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.set_schema_name(schema_name);
    request.mutable_table()->set_name(table_name);
    auto col1 = request.mutable_table()->add_columns();
    col1->set_name("id");
    col1->set_type(::fivetran_sdk::DataType::STRING);

    ::fivetran_sdk::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe the created table
    ::fivetran_sdk::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.set_table_name(table_name);

    {
      // table not found in default "main" schema
      ::fivetran_sdk::DescribeTableResponse response;
      auto status = service.DescribeTable(nullptr, &request, &response);
      REQUIRE_NO_FAIL(status);
      REQUIRE(response.not_found());
    }

    {
      // table found in the correct schema
      request.set_schema_name(schema_name);
      ::fivetran_sdk::DescribeTableResponse response;
      auto status = service.DescribeTable(nullptr, &request, &response);
      REQUIRE_NO_FAIL(status);
      REQUIRE(!response.not_found());

      REQUIRE(response.table().name() == table_name);
      REQUIRE(response.table().columns_size() == 1);
      REQUIRE(response.table().columns(0).name() == "id");
      REQUIRE(response.table().columns(0).type() ==
              ::fivetran_sdk::DataType::STRING);
    }
  }

  {
    // Alter Table
    ::fivetran_sdk::AlterTableRequest request;

    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.set_schema_name(schema_name);
    request.mutable_table()->set_name(table_name);
    ::fivetran_sdk::Column col1;
    col1.set_name("id");
    col1.set_type(::fivetran_sdk::DataType::INT);
    request.mutable_table()->add_columns()->CopyFrom(col1);

    ::fivetran_sdk::AlterTableResponse response;
    auto status = service.AlterTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe the altered table
    ::fivetran_sdk::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.set_table_name(table_name);

    // table found in the correct schema
    request.set_schema_name(schema_name);
    ::fivetran_sdk::DescribeTableResponse response;
    auto status = service.DescribeTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
    REQUIRE(!response.not_found());

    REQUIRE(response.table().name() == table_name);
    REQUIRE(response.table().columns_size() == 1);
    REQUIRE(response.table().columns(0).name() == "id");
    REQUIRE(response.table().columns(0).type() ==
            ::fivetran_sdk::DataType::INT);
  }
}

TEST_CASE("Test fails when database missing", "[integration]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::TestRequest request;
  (*request.mutable_configuration())["motherduck_token"] = "12345";

  ::fivetran_sdk::TestResponse response;

  auto status = service.Test(nullptr, &request, &response);
  REQUIRE_FAIL(status, "Missing property motherduck_database");
}

TEST_CASE("Test fails when token is missing", "[integration]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::TestRequest request;
  (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";

  ::fivetran_sdk::TestResponse response;

  auto status = service.Test(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);
  auto expected_message =
      "Authentication test for database <fivetran_test> failed: Missing "
      "property motherduck_token";
  REQUIRE(status.error_message() == expected_message);
  REQUIRE(response.failure() == expected_message);
}

TEST_CASE("Test endpoint fails when token is bad", "[integration]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::TestRequest request;
  (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
  (*request.mutable_configuration())["motherduck_token"] = "12345";

  ::fivetran_sdk::TestResponse response;

  auto status = service.Test(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);
  CHECK_THAT(status.error_message(),
             Catch::Matchers::ContainsSubstring("UNAUTHENTICATED"));
  CHECK_THAT(status.error_message(),
             Catch::Matchers::ContainsSubstring("UNAUTHENTICATED"));
}

TEST_CASE("Test endpoint succeeds when everything is in order",
          "[integration]") {
  DestinationSdkImpl service;

  ::fivetran_sdk::TestRequest request;
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);
  request.set_name(CONFIG_TEST_NAME_AUTHENTICATE);
  (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
  (*request.mutable_configuration())["motherduck_token"] = token;

  ::fivetran_sdk::TestResponse response;

  auto status = service.Test(nullptr, &request, &response);
  REQUIRE_NO_FAIL(status);
}

template <typename T>
void define_test_table(T &request, const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
  auto col1 = request.mutable_table()->add_columns();
  col1->set_name("id");
  col1->set_type(::fivetran_sdk::DataType::INT);
  col1->set_primary_key(true);

  auto col2 = request.mutable_table()->add_columns();
  col2->set_name("title");
  col2->set_type(::fivetran_sdk::DataType::STRING);

  auto col3 = request.mutable_table()->add_columns();
  col3->set_name("magic_number");
  col3->set_type(::fivetran_sdk::DataType::INT);

  auto col4 = request.mutable_table()->add_columns();
  col4->set_name("_fivetran_deleted");
  col4->set_type(::fivetran_sdk::DataType::BOOLEAN);

  auto col5 = request.mutable_table()->add_columns();
  col5->set_name("_fivetran_synced");
  col5->set_type(::fivetran_sdk::DataType::UTC_DATETIME);
}

std::unique_ptr<duckdb::Connection> get_test_connection(char *token) {
  std::unordered_map<std::string, std::string> props{
      {"motherduck_token", token},
      {"custom_user_agent", "fivetran-integration-test"}};
  duckdb::DBConfig config(props, false);
  duckdb::DuckDB db("md:fivetran_test", &config);
  return std::make_unique<duckdb::Connection>(db);
}

TEST_CASE("WriteBatch", "[integration][current]") {
  DestinationSdkImpl service;

  // Schema will be main
  const std::string table_name = "books" + std::to_string(Catch::rngSeed());

  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  {
    // Create Table
    ::fivetran_sdk::CreateTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    define_test_table(request, table_name);

    ::fivetran_sdk::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  // test connection needs to be created after table creation to avoid stale
  // catalog
  auto con = get_test_connection(token);
  {
    // insert rows from encrypted / compressed file
    ::fivetran_sdk::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.mutable_csv()->set_encryption(::fivetran_sdk::Encryption::AES);
    request.mutable_csv()->set_compression(::fivetran_sdk::Compression::ZSTD);
    define_test_table(request, table_name);
    const std::string filename = "books_batch_1_insert.csv.zst.aes";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    std::ifstream keyfile(filepath + ".key", std::ios::binary);
    char key[33];
    keyfile.read(key, 32);
    key[32] = 0;
    (*request.mutable_keys())[filepath] = key;

    request.add_replace_files(filepath);

    ::fivetran_sdk::WriteBatchResponse response;
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
    ::fivetran_sdk::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    define_test_table(request, table_name);
    const std::string filename = "books_upsert.csv";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    request.add_replace_files(filepath);

    ::fivetran_sdk::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check after upsert
    auto res = con->Query("SELECT id, title, magic_number FROM " + table_name +
                          " ORDER BY id");
    REQUIRE_NO_FAIL(res);

    REQUIRE(res->RowCount() == 3);
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
  }

  {
    // delete
    ::fivetran_sdk::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    define_test_table(request, table_name);
    const std::string filename = "books_delete.csv";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    request.add_delete_files(filepath);

    ::fivetran_sdk::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check after delete
    auto res = con->Query("SELECT id, title, magic_number FROM " + table_name +
                          " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);

    // row 1 got deleted
    REQUIRE(res->GetValue(0, 0) == 2);
    REQUIRE(res->GetValue(1, 0) == "The Two Towers");
    REQUIRE(res->GetValue(2, 0) == 1);

    REQUIRE(res->GetValue(0, 1) == 3);
    REQUIRE(res->GetValue(1, 1) == "The Hobbit");
    REQUIRE(res->GetValue(2, 1) == 14);
  }

  {
    // update
    ::fivetran_sdk::WriteBatchRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.mutable_csv()->set_unmodified_string(
        "unmod-NcK9NIjPUutCsz4mjOQQztbnwnE1sY3");
    define_test_table(request, table_name);
    const std::string filename = "books_update.csv";
    const std::string filepath = TEST_RESOURCES_DIR + filename;

    request.add_update_files(filepath);

    ::fivetran_sdk::WriteBatchResponse response;
    auto status = service.WriteBatch(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // check after update
    auto res = con->Query("SELECT id, title, magic_number FROM " + table_name +
                          " ORDER BY id");
    REQUIRE_NO_FAIL(res);
    REQUIRE(res->RowCount() == 2);

    REQUIRE(res->GetValue(0, 0) == 2);
    REQUIRE(res->GetValue(1, 0) == "The empire strikes back");
    REQUIRE(res->GetValue(2, 0) == 1);

    REQUIRE(res->GetValue(0, 1) == 3);
    REQUIRE(res->GetValue(1, 1) == "The Hobbit");
    REQUIRE(res->GetValue(2, 1) == 15); // updated value
  }

  {
    // truncate data before Jan 9 2024
    ::fivetran_sdk::TruncateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.set_table_name(table_name);
    request.set_synced_column("_fivetran_synced");
    request.mutable_soft()->set_deleted_column("_fivetran_deleted");

    const auto cutoff_datetime = 1707436800; // 2024-02-09 0:0:0 GMT, trust me
    request.mutable_utc_delete_before()->set_seconds(cutoff_datetime);
    request.mutable_utc_delete_before()->set_nanos(0);
    ::fivetran_sdk::TruncateResponse response;
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
    REQUIRE(res->RowCount() == 2);
  }

  {
    // truncate table does nothing if there is no utc_delete_before field set
    ::fivetran_sdk::TruncateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.set_table_name(table_name);
    request.set_synced_column("_fivetran_synced");
    request.mutable_soft()->set_deleted_column("_fivetran_deleted");

    ::fivetran_sdk::TruncateResponse response;
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
    REQUIRE(res->RowCount() == 2);
  }

  {
    // hard truncate all data (deleted_column not set in request)
    ::fivetran_sdk::TruncateRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.set_table_name(table_name);
    request.set_synced_column("_fivetran_synced");

    const auto cutoff_datetime =
        1893456000; // delete everything before 2030-01-01
    request.mutable_utc_delete_before()->set_seconds(cutoff_datetime);
    request.mutable_utc_delete_before()->set_nanos(0);
    ::fivetran_sdk::TruncateResponse response;
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

TEST_CASE("CreateTable with multiple primary keys", "[integration]") {
  DestinationSdkImpl service;

  const std::string table_name =
      "multikey_table" + std::to_string(Catch::rngSeed());
  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  {
    // Create Table
    ::fivetran_sdk::CreateTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.mutable_table()->set_name(table_name);
    auto col1 = request.mutable_table()->add_columns();
    col1->set_name("id1");
    col1->set_type(::fivetran_sdk::DataType::INT);
    col1->set_primary_key(true);
    auto col2 = request.mutable_table()->add_columns();
    col2->set_name("id2");
    col2->set_type(::fivetran_sdk::DataType::INT);
    col2->set_primary_key(true);

    ::fivetran_sdk::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe the created table
    ::fivetran_sdk::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.set_table_name(table_name);

    {
      ::fivetran_sdk::DescribeTableResponse response;
      auto status = service.DescribeTable(nullptr, &request, &response);
      REQUIRE_NO_FAIL(status);
      REQUIRE(response.table().columns().size() == 2);
    }
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
    ::fivetran_sdk::CreateTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.mutable_table()->set_name(table_name);
    auto col1 = request.mutable_table()->add_columns();
    col1->set_name("data");
    col1->set_type(::fivetran_sdk::DataType::JSON);

    ::fivetran_sdk::CreateTableResponse response;
    auto status = service.CreateTable(nullptr, &request, &response);
    REQUIRE_NO_FAIL(status);
  }

  {
    // Describe the created table
    ::fivetran_sdk::DescribeTableRequest request;
    (*request.mutable_configuration())["motherduck_token"] = token;
    (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
    request.set_table_name(table_name);

    {
      ::fivetran_sdk::DescribeTableResponse response;
      auto status = service.DescribeTable(nullptr, &request, &response);
      REQUIRE_NO_FAIL(status);
      REQUIRE(response.table().columns().size() == 1);
      REQUIRE(response.table().columns(0).type() ==
              ::fivetran_sdk::DataType::STRING);
    }
  }
}

TEST_CASE("Truncate nonexistent table should succeed", "[integration]") {
  DestinationSdkImpl service;

  const std::string bad_table_name = "nonexistent";

  auto token = std::getenv("motherduck_token");
  REQUIRE(token);

  ::fivetran_sdk::TruncateRequest request;
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
  request.set_schema_name("some_schema");
  request.set_table_name(bad_table_name);
  request.set_synced_column("_fivetran_synced");
  request.mutable_soft()->set_deleted_column("_fivetran_deleted");

  ::fivetran_sdk::TruncateResponse response;

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

  ::fivetran_sdk::TruncateRequest request;
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] = "fivetran_test";
  request.set_table_name("some_table");

  ::fivetran_sdk::TruncateResponse response;
  auto status = service.Truncate(nullptr, &request, &response);

  REQUIRE_FAIL(status, "Synced column is required");
}