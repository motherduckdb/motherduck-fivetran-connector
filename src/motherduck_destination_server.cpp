#include "motherduck_destination_server.hpp"
#include "csv_processor.hpp"
#include "decryption.hpp"
#include "destination_sdk.grpc.pb.h"
#include "duckdb.hpp"
#include "fivetran_duckdb_interop.hpp"
#include "ingest_properties.hpp"
#include "md_logging.hpp"
#include "sql_generator.hpp"
#include "temp_database.hpp"

#include <filesystem>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <string>

std::string
find_property(const google::protobuf::Map<std::string, std::string> &config,
              const std::string &property_name) {
  auto token_it = config.find(property_name);
  if (token_it == config.end()) {
    throw std::invalid_argument("Missing property " + property_name);
  }
  return token_it->second;
}

int find_optional_property(
    const google::protobuf::Map<std::string, std::string> &config,
    const std::string &property_name, int default_value,
    const std::function<int(const std::string &)> &parse) {
  auto token_it = config.find(property_name);
  return token_it == config.end() || token_it->second.empty()
             ? default_value
             : parse(token_it->second);
}

template <typename T> std::string get_schema_name(const T *request) {
  std::string schema_name = request->schema_name();
  if (schema_name.empty()) {
    return "main";
  }
  return schema_name;
}

template <typename T> std::string get_table_name(const T *request) {
  std::string table_name = request->table_name();
  if (table_name.empty()) {
    throw std::invalid_argument("Table name cannot be empty");
  }
  return table_name;
}

std::vector<column_def> get_duckdb_columns(
    const google::protobuf::RepeatedPtrField<fivetran_sdk::v2::Column>
        &fivetran_columns) {
  std::vector<column_def> duckdb_columns;
  for (auto &col : fivetran_columns) {
    // todo: if not decimal? (hasDecimal())
    const auto ddbtype = get_duckdb_type(col.type());
    if (ddbtype == duckdb::LogicalTypeId::INVALID) {
      throw std::invalid_argument("Cannot convert Fivetran type <" +
                                  DataType_Name(col.type()) + "> for column <" +
                                  col.name() + "> to a DuckDB type");
    }
    auto precision = col.has_params() && col.params().has_decimal()
                         ? col.params().decimal().precision()
                         : DUCKDB_DEFAULT_PRECISION;
    auto scale = col.has_params() && col.params().has_decimal()
                     ? col.params().decimal().scale()
                     : DUCKDB_DEFAULT_SCALE;
    duckdb_columns.push_back(
        column_def{col.name(), ddbtype, col.primary_key(), precision, scale});
  }
  return duckdb_columns;
}

duckdb::DuckDB &
DestinationSdkImpl::get_duckdb(const std::string &md_token,
                               const std::string &db_name,
                               const std::shared_ptr<mdlog::MdLog> &logger) {
  auto initialize_db = [this, &md_token, &db_name, &logger]() {
    duckdb::DBConfig config;
    config.SetOptionByName(MD_PROP_TOKEN, md_token);
    config.SetOptionByName("custom_user_agent",
                           std::string("fivetran/") + GIT_COMMIT_SHA);
    config.SetOptionByName("old_implicit_casting", true);
    config.SetOptionByName("motherduck_attach_mode", "single");
    logger->info("    initialize_db: created configuration");

    db = duckdb::DuckDB("md:" + db_name, &config);
    logger->info("    initialize_db: created database instance");

    duckdb::Connection con(db);

    const auto load_res = con.Query("LOAD core_functions");
    if (load_res->HasError()) {
      throw std::runtime_error("Could not LOAD core_functions: " +
                               load_res->GetError());
    }
    logger->info("    initialize_db: loaded core_functions");

    const auto load_res2 = con.Query("LOAD parquet");
    if (load_res2->HasError()) {
      throw std::runtime_error("Could not LOAD parquet: " +
                               load_res2->GetError());
    }
    logger->info("    initialize_db: loaded parquet");

    initial_md_token = md_token;
  };

  std::call_once(db_init_flag, initialize_db);

  if (md_token != initial_md_token) {
    throw std::runtime_error("Trying to connect to MotherDuck with a different "
                             "token than initially provided");
  }

  return db;
}

// todo: rename to init_connection
std::unique_ptr<duckdb::Connection> DestinationSdkImpl::get_connection(
    const google::protobuf::Map<std::string, std::string> &request_config,
    const std::string &db_name, const std::shared_ptr<mdlog::MdLog> &logger) {
  logger->info("    get_connection: start");
  const std::string md_token = find_property(request_config, MD_PROP_TOKEN);
  logger->info("    get_connection: got token");

  duckdb::DuckDB &db = get_duckdb(md_token, db_name, logger);
  auto con = std::make_unique<duckdb::Connection>(db);
  logger->info("    get_connection: created connection");

  const auto client_ids_res =
      con->Query("SELECT md_current_client_duckdb_id(), "
                 "md_current_client_connection_id()");
  if (client_ids_res->HasError()) {
    logger->warning(
        "Could not retrieve the current DuckDB and connection ID: " +
        client_ids_res->GetError());
  } else {
    logger->info("    get_connection: about to set duckdb_id in logger");
    logger->set_duckdb_id(client_ids_res->GetValue(0, 0).ToString());
    logger->info("    get_connection: about to set connection_id in logger");
    logger->set_connection_id(client_ids_res->GetValue(1, 0).ToString());
  }

  // Set default_collation to a connection-specific default value which
  // overwrites any global setting and ensures that client-side planning and
  // server-side execution use the same collation.
  const auto set_collation_res = con->Query("SET default_collation=''");
  if (set_collation_res->HasError()) {
    throw std::runtime_error(
        "    get_connection: Could not SET default_collation: " +
        set_collation_res->GetError());
  }

  logger->info("    get_connection: all done, returning connection");
  return con;
}

std::string
get_encryption_key(const std::string &filename,
                   const google::protobuf::Map<std::string, std::string> &keys,
                   ::fivetran_sdk::v2::Encryption encryption) {
  if (encryption == ::fivetran_sdk::v2::Encryption::NONE) {
    return "";
  }
  auto encryption_key_it = keys.find(filename);

  if (encryption_key_it == keys.end()) {
    throw std::invalid_argument("Missing encryption key for " + filename);
  }

  return encryption_key_it->second;
}

template <typename T>
IngestProperties
create_ingest_props(const std::string &filename, const T &request,
                    const std::vector<column_def> &cols,
                    const int csv_block_size,
                    const UnmodifiedMarker allow_unmodified_string,
                    const std::string &temp_db_name) {
  const std::string decryption_key = get_encryption_key(
      filename, request->keys(), request->file_params().encryption());
  return IngestProperties(filename, decryption_key, cols,
                          request->file_params().null_string(), csv_block_size,
                          allow_unmodified_string, temp_db_name);
}

grpc::Status DestinationSdkImpl::ConfigurationForm(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::v2::ConfigurationFormRequest *request,
    ::fivetran_sdk::v2::ConfigurationFormResponse *response) {

  response->set_schema_selection_supported(true);
  response->set_table_selection_supported(true);

  fivetran_sdk::v2::FormField token_field;
  token_field.set_name(MD_PROP_TOKEN);
  token_field.set_label("Authentication Token");
  token_field.set_description(
      "Please get your authentication token from app.motherduck.com");
  token_field.set_text_field(fivetran_sdk::v2::Password);
  token_field.set_required(true);

  response->add_fields()->CopyFrom(token_field);

  fivetran_sdk::v2::FormField db_field;
  db_field.set_name(MD_PROP_DATABASE);
  db_field.set_label("Database Name");
  db_field.set_description("The database to work in");
  db_field.set_text_field(fivetran_sdk::v2::PlainText);
  db_field.set_required(true);

  response->add_fields()->CopyFrom(db_field);

  fivetran_sdk::v2::FormField block_size_field;
  block_size_field.set_name(MD_PROP_CSV_BLOCK_SIZE);
  block_size_field.set_label(
      "Maximum individual value size, in megabytes (default 1 MB)");
  block_size_field.set_description(
      "This field limits the maximum length of a single field value coming "
      "from the input source."
      "Must be a valid numeric value");
  block_size_field.set_text_field(fivetran_sdk::v2::PlainText);
  block_size_field.set_required(false);
  response->add_fields()->CopyFrom(block_size_field);

  auto block_size_test = response->add_tests();
  block_size_test->set_name(CONFIG_TEST_NAME_CSV_BLOCK_SIZE);
  block_size_test->set_label("Maximum value size is a valid number");

  auto connection_test = response->add_tests();
  connection_test->set_name(CONFIG_TEST_NAME_AUTHENTICATE);
  connection_test->set_label("Test Authentication");

  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status DestinationSdkImpl::Capabilities(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::v2::CapabilitiesRequest *request,
    ::fivetran_sdk::v2::CapabilitiesResponse *response) {
  response->set_batch_file_format(::fivetran_sdk::v2::CSV);
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status DestinationSdkImpl::DescribeTable(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::v2::DescribeTableRequest *request,
    ::fivetran_sdk::v2::DescribeTableResponse *response) {
  auto logger = std::make_shared<mdlog::MdLog>();
  try {
    logger->info("Endpoint <DescribeTable>: started");
    std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name, logger);
    logger->info("Endpoint <DescribeTable>: got connection");
    auto sql_generator = std::make_unique<MdSqlGenerator>(logger);
    table_def table_name{db_name, get_schema_name(request),
                         get_table_name(request)};
    logger->info("Endpoint <DescribeTable>: schema name <" +
                 table_name.schema_name + ">");
    logger->info("Endpoint <DescribeTable>: table name <" +
                 table_name.table_name + ">");
    if (!sql_generator->table_exists(*con, table_name)) {
      logger->info("Endpoint <DescribeTable>: table not found");
      response->set_not_found(true);
      return ::grpc::Status(::grpc::StatusCode::OK, "");
    }

    logger->info("Endpoint <DescribeTable>: table exists; getting columns");
    auto duckdb_columns = sql_generator->describe_table(*con, table_name);
    logger->info("Endpoint <DescribeTable>: got " +
                 std::to_string(duckdb_columns.size()) + " columns");

    fivetran_sdk::v2::Table *table = response->mutable_table();
    table->set_name(get_table_name(request));

    for (auto &col : duckdb_columns) {
      logger->info("Endpoint <DescribeTable>:   processing column " + col.name);
      fivetran_sdk::v2::Column *ft_col = table->mutable_columns()->Add();
      ft_col->set_name(col.name);
      const auto fivetran_type = get_fivetran_type(col.type);
      logger->info("Endpoint <DescribeTable>:   column type = " +
                   std::to_string(fivetran_type));
      ft_col->set_type(fivetran_type);
      ft_col->set_primary_key(col.primary_key);
      if (fivetran_type == fivetran_sdk::v2::DECIMAL) {
        ft_col->mutable_params()->mutable_decimal()->set_precision(col.width);
        ft_col->mutable_params()->mutable_decimal()->set_scale(col.scale);
      }
    }

  } catch (const std::exception &e) {
    logger->severe("DescribeTable endpoint failed for schema <" +
                   request->schema_name() + ">, table <" +
                   request->table_name() + ">:" + std::string(e.what()));
    response->mutable_task()->set_message(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  logger->info("Endpoint <DescribeTable>: ended");
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status DestinationSdkImpl::CreateTable(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::v2::CreateTableRequest *request,
    ::fivetran_sdk::v2::CreateTableResponse *response) {

  auto logger = std::make_shared<mdlog::MdLog>();
  try {
    logger->info("Endpoint <CreateTable>: started");
    auto schema_name = get_schema_name(request);

    std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name, logger);
    auto sql_generator = std::make_unique<MdSqlGenerator>(logger);
    const table_def table{db_name, schema_name, request->table().name()};

    if (!sql_generator->schema_exists(*con, db_name, schema_name)) {
      sql_generator->create_schema(*con, db_name, schema_name);
    }

    const auto cols = get_duckdb_columns(request->table().columns());
    sql_generator->create_table(*con, table, cols, {});
    response->set_success(true);
  } catch (const std::exception &e) {
    logger->severe("CreateTable endpoint failed for schema <" +
                   request->schema_name() + ">, table <" +
                   request->table().name() + ">:" + std::string(e.what()));
    response->mutable_task()->set_message(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  logger->info("Endpoint <CreateTable>: ended");
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status DestinationSdkImpl::AlterTable(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::v2::AlterTableRequest *request,
    ::fivetran_sdk::v2::AlterTableResponse *response) {
  auto logger = std::make_shared<mdlog::MdLog>();
  try {
    logger->info("Endpoint <AlterTable>: started");
    std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    table_def table_name{db_name, get_schema_name(request),
                         request->table().name()};

    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name, logger);
    auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

    sql_generator->alter_table(*con, table_name,
                               get_duckdb_columns(request->table().columns()));
    response->set_success(true);
  } catch (const std::exception &e) {
    logger->severe("AlterTable endpoint failed for schema <" +
                   request->schema_name() + ">, table <" +
                   request->table().name() + ">:" + std::string(e.what()));
    response->mutable_task()->set_message(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  logger->info("Endpoint <AlterTable>: ended");
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status
DestinationSdkImpl::Truncate(::grpc::ServerContext *context,
                             const ::fivetran_sdk::v2::TruncateRequest *request,
                             ::fivetran_sdk::v2::TruncateResponse *response) {

  auto logger = std::make_shared<mdlog::MdLog>();
  try {
    logger->info("Endpoint <Truncate>: started");
    std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    table_def table_name{db_name, get_schema_name(request),
                         get_table_name(request)};
    if (request->synced_column().empty()) {
      throw std::invalid_argument("Synced column is required");
    }

    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name, logger);
    auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

    if (sql_generator->table_exists(*con, table_name)) {
      std::chrono::nanoseconds delete_before_ts =
          std::chrono::seconds(request->utc_delete_before().seconds()) +
          std::chrono::nanoseconds(request->utc_delete_before().nanos());
      const std::string deleted_column =
          request->has_soft() ? request->soft().deleted_column() : "";
      sql_generator->truncate_table(*con, table_name, request->synced_column(),
                                    delete_before_ts, deleted_column);
    } else {
      logger->warning("Table <" + request->table_name() +
                      "> not found in schema <" + request->schema_name() +
                      ">; not truncated");
    }

  } catch (const std::exception &e) {
    logger->severe("Truncate endpoint failed for schema <" +
                   request->schema_name() + ">, table <" +
                   request->table_name() + ">:" + std::string(e.what()));
    response->mutable_task()->set_message(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  logger->info("Endpoint <Truncate>: ended");
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status DestinationSdkImpl::WriteBatch(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::v2::WriteBatchRequest *request,
    ::fivetran_sdk::v2::WriteBatchResponse *response) {

  auto logger = std::make_shared<mdlog::MdLog>();
  try {
    logger->info("Endpoint <WriteBatch>: started");
    auto schema_name = get_schema_name(request);

    const std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    const int csv_block_size = find_optional_property(
        request->configuration(), MD_PROP_CSV_BLOCK_SIZE, 1,
        [&](const std::string &val) -> int { return std::stoi(val); });
    logger->info("CSV BLOCK SIZE = " + std::to_string(csv_block_size));

    table_def table_name{db_name, get_schema_name(request),
                         request->table().name()};
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name, logger);
    auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

    const auto cols = get_duckdb_columns(request->table().columns());
    std::vector<const column_def *> columns_pk;
    std::vector<const column_def *> columns_regular;
    find_primary_keys(cols, columns_pk, &columns_regular);

    if (columns_pk.empty()) {
      throw std::invalid_argument("No primary keys found");
    }

    TempDatabase temp_db(*con, logger);

    for (auto &filename : request->replace_files()) {
      logger->info("Processing replace file " + filename);
      const auto decryption_key = get_encryption_key(
          filename, request->keys(), request->file_params().encryption());

      IngestProperties props(
          filename, decryption_key, cols, request->file_params().null_string(),
          csv_block_size, UnmodifiedMarker::Disallowed, temp_db.name);

      csv_processor::ProcessFile(
          *con, props, logger, [&](const std::string &view_name) {
            sql_generator->upsert(*con, table_name, view_name, columns_pk,
                                  columns_regular);
          });
    }

    for (auto &filename : request->update_files()) {
      logger->info("Processing update file " + filename);
      auto decryption_key = get_encryption_key(
          filename, request->keys(), request->file_params().encryption());
      IngestProperties props(
          filename, decryption_key, cols, request->file_params().null_string(),
          csv_block_size, UnmodifiedMarker::Allowed, temp_db.name);

      csv_processor::ProcessFile(
          *con, props, logger, [&](const std::string &view_name) {
            sql_generator->update_values(
                *con, table_name, view_name, columns_pk, columns_regular,
                request->file_params().unmodified_string());
          });
    }

    for (auto &filename : request->delete_files()) {
      logger->info("Processing delete file " + filename);
      auto decryption_key = get_encryption_key(
          filename, request->keys(), request->file_params().encryption());
      std::vector<column_def> cols_to_read;
      for (const auto &col : columns_pk) {
        cols_to_read.push_back(*col);
      }
      IngestProperties props(filename, decryption_key, cols_to_read,
                             request->file_params().null_string(),
                             csv_block_size, UnmodifiedMarker::Disallowed,
                             temp_db.name);

      csv_processor::ProcessFile(
          *con, props, logger, [&](const std::string &view_name) {
            sql_generator->delete_rows(*con, table_name, view_name, columns_pk);
          });
    }

  } catch (const std::exception &e) {

    auto const msg = "WriteBatch endpoint failed for schema <" +
                     request->schema_name() + ">, table <" +
                     request->table().name() + ">:" + std::string(e.what());
    logger->severe(msg);
    response->mutable_task()->set_message(msg);
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, msg);
  }

  logger->info("Endpoint <WriteBatch>: ended");
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

::grpc::Status DestinationSdkImpl::WriteHistoryBatch(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::v2::WriteHistoryBatchRequest *request,
    ::fivetran_sdk::v2::WriteBatchResponse *response) {

  auto logger = std::make_shared<mdlog::MdLog>();

  try {
    logger->info("Endpoint <WriteHistoryBatch>: started");
    auto schema_name = get_schema_name(request);

    const std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    const int csv_block_size = find_optional_property(
        request->configuration(), MD_PROP_CSV_BLOCK_SIZE, 1,
        [&](const std::string &val) -> int { return std::stoi(val); });
    logger->info("CSV BLOCK SIZE = " + std::to_string(csv_block_size));

    table_def table_name{db_name, get_schema_name(request),
                         request->table().name()};
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name, logger);
    auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

    const auto cols = get_duckdb_columns(request->table().columns());
    std::vector<const column_def *> columns_pk;
    std::vector<const column_def *> columns_regular;
    find_primary_keys(cols, columns_pk, &columns_regular, "_fivetran_start");

    if (columns_pk.empty()) {
      throw std::invalid_argument("No primary keys found");
    }

    TempDatabase temp_db(*con, logger);

    // delete overlapping records
    for (auto &filename : request->earliest_start_files()) {
      logger->info("Processing earliest start file " + filename);
      // "This file contains a single record for each primary key in the
      // incoming batch, with the earliest _fivetran_start"
      std::vector<column_def> earliest_start_cols;
      earliest_start_cols.reserve(columns_pk.size() + 1);
      for (const auto &col : columns_pk) {
        earliest_start_cols.push_back(*col);
      }
      earliest_start_cols.push_back(
          {.name = "_fivetran_start",
           .type = duckdb::LogicalTypeId::TIMESTAMP_TZ});
      IngestProperties props = create_ingest_props(
          filename, request, earliest_start_cols, csv_block_size,
          UnmodifiedMarker::Disallowed, temp_db.name);

      csv_processor::ProcessFile(
          *con, props, logger, [&](const std::string &view_name) {
            sql_generator->deactivate_historical_records(
                *con, table_name, view_name, columns_pk, temp_db.name);
          });
    }

    for (auto &filename : request->update_files()) {
      logger->info("update file " + filename);
      IngestProperties props =
          create_ingest_props(filename, request, cols, csv_block_size,
                              UnmodifiedMarker::Allowed, temp_db.name);

      csv_processor::ProcessFile(
          *con, props, logger, [&](const std::string &view_name) {
            sql_generator->add_partial_historical_values(
                *con, table_name, view_name, columns_pk, columns_regular,
                request->file_params().unmodified_string(), temp_db.name);
          });
    }

    // upsert files
    for (auto &filename : request->replace_files()) {
      logger->info("replace/upsert file " + filename);
      IngestProperties props =
          create_ingest_props(filename, request, cols, csv_block_size,
                              UnmodifiedMarker::Disallowed, temp_db.name);
      csv_processor::ProcessFile(
          *con, props, logger, [&](const std::string &view_name) {
            sql_generator->upsert(*con, table_name, view_name, columns_pk,
                                  columns_regular);
          });
    }

    for (auto &filename : request->delete_files()) {
      logger->info("delete file " + filename);
      IngestProperties props =
          create_ingest_props(filename, request, cols, csv_block_size,
                              UnmodifiedMarker::Disallowed, temp_db.name);

      csv_processor::ProcessFile(*con, props, logger,
                                 [&](const std::string &view_name) {
                                   sql_generator->delete_historical_rows(
                                       *con, table_name, view_name, columns_pk);
                                 });
    }
  } catch (const std::exception &e) {

    auto const msg = "WriteBatch endpoint failed for schema <" +
                     request->schema_name() + ">, table <" +
                     request->table().name() + ">:" + std::string(e.what());
    logger->severe(msg);
    response->mutable_task()->set_message(msg);
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, msg);
  }

  logger->info("Endpoint <WriteHistoryBatch>: ended");
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

void check_csv_block_size_is_numeric(
    const google::protobuf::Map<std::string, std::string> &config) {
  auto token_it = config.find(MD_PROP_CSV_BLOCK_SIZE);

  // missing token is fine but non-numeric token isn't
  if (token_it != config.end() &&
      token_it->second.find_first_not_of("0123456789") != std::string::npos) {
    throw std::runtime_error(
        "Maximum individual value size must be numeric if present");
  }
}

grpc::Status
DestinationSdkImpl::Test(::grpc::ServerContext *context,
                         const ::fivetran_sdk::v2::TestRequest *request,
                         ::fivetran_sdk::v2::TestResponse *response) {

  auto logger = std::make_shared<mdlog::MdLog>();
  std::string db_name;
  try {
    db_name = find_property(request->configuration(), MD_PROP_DATABASE);
  } catch (const std::exception &e) {
    auto msg = "Test endpoint failed; could not retrieve database name: " +
               std::string(e.what());
    logger->severe(msg);
    response->set_success(false);
    response->set_failure(msg);
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, e.what());
  }

  try {
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name, logger);
    auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

    if (request->name() == CONFIG_TEST_NAME_AUTHENTICATE) {
      sql_generator->check_connection(*con);
    } else if (request->name() == CONFIG_TEST_NAME_CSV_BLOCK_SIZE) {
      check_csv_block_size_is_numeric(request->configuration());
    } else {
      auto const msg = "Unknown test requested: <" + request->name() + ">";
      logger->severe(msg);
      response->set_success(false);
      response->set_failure(msg);
      return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, msg);
    }
  } catch (const std::exception &e) {
    auto msg = "Test <" + request->name() + "> for database <" + db_name +
               "> failed: " + std::string(e.what());
    response->set_success(false);
    response->set_failure(msg);
    // grpc call succeeded; the response reflects config test failure
    return ::grpc::Status(::grpc::StatusCode::OK, msg);
  }

  response->set_success(true);
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}
