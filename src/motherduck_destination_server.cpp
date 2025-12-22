#include "motherduck_destination_server.hpp"

#include "config.hpp"
#include "config_tester.hpp"
#include "csv_processor.hpp"
#include "decryption.hpp"
#include "destination_sdk.grpc.pb.h"
#include "duckdb.hpp"
#include "fivetran_duckdb_interop.hpp"
#include "ingest_properties.hpp"
#include "md_logging.hpp"
#include "sql_generator.hpp"
#include "temp_database.hpp"

#include <exception>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <string>

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

    constexpr std::uint32_t DUCKDB_DEFAULT_PRECISION = 18;
    constexpr std::uint32_t DUCKDB_DEFAULT_SCALE = 3;

    const auto precision = col.has_params() && col.params().has_decimal()
                               ? col.params().decimal().precision()
                               : DUCKDB_DEFAULT_PRECISION;
    const auto scale = col.has_params() && col.params().has_decimal()
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
    config.SetOptionByName(config::PROP_TOKEN, md_token);
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

std::unique_ptr<duckdb::Connection> DestinationSdkImpl::get_connection(
    const google::protobuf::Map<std::string, std::string> &request_config,
    const std::string &db_name, const std::shared_ptr<mdlog::MdLog> &logger) {
  logger->info("    get_connection: start");
  const std::string md_token =
      config::find_property(request_config, config::PROP_TOKEN);
  logger->info("    get_connection: got token");

  duckdb::DuckDB &db = get_duckdb(md_token, db_name, logger);
  auto con = std::make_unique<duckdb::Connection>(db);
  logger->info("    get_connection: created connection");

  // This query triggers a welcome pack fetch
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

  // Set the time zone to UTC. This can be removed once we do not load ICU
  // anymore.
  const auto set_timezone_res = con->Query("SET timezone='UTC'");
  if (set_timezone_res->HasError()) {
    throw std::runtime_error("    get_connection: Could not SET TimeZone: " +
                             set_timezone_res->GetError());
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
                    const UnmodifiedMarker allow_unmodified_string,
                    const std::string &temp_db_name) {
  const std::string decryption_key = get_encryption_key(
      filename, request->keys(), request->file_params().encryption());
  return IngestProperties(filename, decryption_key, cols,
                          request->file_params().null_string(),
                          allow_unmodified_string, temp_db_name);
}

grpc::Status DestinationSdkImpl::ConfigurationForm(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::v2::ConfigurationFormRequest *request,
    ::fivetran_sdk::v2::ConfigurationFormResponse *response) {

  response->set_schema_selection_supported(true);
  response->set_table_selection_supported(true);

  fivetran_sdk::v2::FormField token_field;
  token_field.set_name(config::PROP_TOKEN);
  token_field.set_label("Authentication Token");
  token_field.set_description(
      "Please get your authentication token from app.motherduck.com");
  token_field.set_text_field(fivetran_sdk::v2::Password);
  token_field.set_required(true);
  response->add_fields()->CopyFrom(token_field);

  fivetran_sdk::v2::FormField db_field;
  db_field.set_name(config::PROP_DATABASE);
  db_field.set_label("Database Name");
  db_field.set_description("The database to work in. The database must already "
                           "exist and be writable.");
  db_field.set_text_field(fivetran_sdk::v2::PlainText);
  db_field.set_required(true);
  response->add_fields()->CopyFrom(db_field);

  for (const auto &test_case : config_tester::get_test_cases()) {
    auto connection_test = response->add_tests();
    connection_test->set_name(test_case.name);
    connection_test->set_label(test_case.description);
  }

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
    const std::string db_name =
        config::find_property(request->configuration(), config::PROP_DATABASE);
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

    const std::string db_name =
        config::find_property(request->configuration(), config::PROP_DATABASE);
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
    const std::string db_name =
        config::find_property(request->configuration(), config::PROP_DATABASE);
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
    const std::string db_name =
        config::find_property(request->configuration(), config::PROP_DATABASE);
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
        config::find_property(request->configuration(), config::PROP_DATABASE);

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

      IngestProperties props(filename, decryption_key, cols,
                             request->file_params().null_string(),
                             UnmodifiedMarker::Disallowed, temp_db.name);

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
      IngestProperties props(filename, decryption_key, cols,
                             request->file_params().null_string(),
                             UnmodifiedMarker::Allowed, temp_db.name);

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
                             UnmodifiedMarker::Disallowed, temp_db.name);

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
        config::find_property(request->configuration(), config::PROP_DATABASE);

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
      IngestProperties props =
          create_ingest_props(filename, request, earliest_start_cols,
                              UnmodifiedMarker::Disallowed, temp_db.name);
      csv_processor::ProcessFile(
          *con, props, logger, [&](const std::string &view_name) {
            sql_generator->deactivate_historical_records(
                *con, table_name, view_name, columns_pk, temp_db.name);
          });
    }

    for (auto &filename : request->update_files()) {
      logger->info("update file " + filename);
      IngestProperties props = create_ingest_props(
          filename, request, cols, UnmodifiedMarker::Allowed, temp_db.name);

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
      IngestProperties props = create_ingest_props(
          filename, request, cols, UnmodifiedMarker::Disallowed, temp_db.name);
      csv_processor::ProcessFile(
          *con, props, logger, [&](const std::string &view_name) {
            sql_generator->insert(*con, table_name, view_name, columns_pk,
                                  columns_regular);
          });
    }

    for (auto &filename : request->delete_files()) {
      logger->info("delete file " + filename);
      IngestProperties props = create_ingest_props(
          filename, request, cols, UnmodifiedMarker::Disallowed, temp_db.name);

      csv_processor::ProcessFile(*con, props, logger,
                                 [&](const std::string &view_name) {
                                   sql_generator->delete_historical_rows(
                                       *con, table_name, view_name, columns_pk);
                                 });
    }
  } catch (const std::exception &e) {

    auto const msg = "WriteHistoryBatch endpoint failed for schema <" +
                     request->schema_name() + ">, table <" +
                     request->table().name() + ">:" + std::string(e.what());
    logger->severe(msg);
    response->mutable_task()->set_message(msg);
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, msg);
  }

  logger->info("Endpoint <WriteHistoryBatch>: ended");
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

std::string extract_readable_error(const std::exception &ex) {
  // DuckDB errors are JSON strings. Converting it to ErrorData to extract the
  // message.
  duckdb::ErrorData error(ex.what());
  std::string error_message = error.RawMessage();

  // Errors thrown in the initialization function are very verbose. Example:
  // Invalid Input Error: Initialization function "motherduck_duckdb_cpp_init"
  // from file "motherduck.duckdb_extension" threw an exception: "Failed to
  // attach 'my_db': no database/share named 'my_db' found". We are only
  // interested in the last part.
  const std::string boilerplate =
      "Initialization function \"motherduck_duckdb_cpp_init\" from file";
  if (error_message.find(boilerplate) != std::string::npos) {
    const std::string search_string = "threw an exception: ";
    const auto pos = error_message.find(search_string);
    if (pos != std::string::npos) {
      error_message = "Connection to MotherDuck failed: " +
                      error_message.substr(pos + search_string.length());
    }
  }

  const std::string not_found_error_substr = "no database/share named";
  if (error_message.find(not_found_error_substr) != std::string::npos) {
    // Remove the quotation mark at the end
    error_message = error_message.substr(0, error_message.length() - 1);
    // Full error: "no database/share named 'my_db' found. Create it first in
    // your MotherDuck account."
    error_message += ". Create it first in your MotherDuck account.\"";
  }

  return error_message;
}

grpc::Status
DestinationSdkImpl::Test(::grpc::ServerContext *context,
                         const ::fivetran_sdk::v2::TestRequest *request,
                         ::fivetran_sdk::v2::TestResponse *response) {
  const std::string test_name = request->name();
  const std::string error_prefix = "Test <" + test_name + "> failed: ";
  const auto user_config = request->configuration();

  std::unique_ptr<duckdb::Connection> con;
  // This function already loads the extension and connects to MotherDuck.
  // If this fails, we catch the exception and rewrite it a bit to make
  // it more actionable.
  try {
    const std::string db_name =
        config::find_property(user_config, config::PROP_DATABASE);
    const auto logger = std::make_shared<mdlog::MdLog>();
    con = get_connection(user_config, db_name, logger);
  } catch (const std::exception &ex) {
    const auto error_message = extract_readable_error(ex);
    response->set_failure(error_prefix + error_message);
    return ::grpc::Status::OK;
  }

  // Run actual tests
  try {
    auto test_result = config_tester::run_test(test_name, *con);
    if (test_result.success) {
      response->set_success(true);
    } else {
      response->set_failure(error_prefix + test_result.failure_message);
    }
  } catch (const std::exception &ex) {
    const auto error_message = extract_readable_error(ex);
    response->set_failure(error_prefix + error_message);
  }
  return ::grpc::Status::OK;
}
