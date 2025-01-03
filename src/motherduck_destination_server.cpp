#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include <arrow/c/bridge.h>
#include <grpcpp/grpcpp.h>

#include <common.hpp>
#include <csv_arrow_ingest.hpp>
#include <destination_sdk.grpc.pb.h>
#include <fivetran_duckdb_interop.hpp>
#include <md_logging.hpp>
#include <motherduck_destination_server.hpp>
#include <sql_generator.hpp>

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
    const google::protobuf::RepeatedPtrField<fivetran_sdk::Column>
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
    auto precision = col.has_decimal() ? col.decimal().precision()
                                       : DUCKDB_DEFAULT_PRECISION;
    auto scale =
        col.has_decimal() ? col.decimal().scale() : DUCKDB_DEFAULT_SCALE;
    duckdb_columns.push_back(
        column_def{col.name(), ddbtype, col.primary_key(), precision, scale});
  }
  return duckdb_columns;
}

// todo: rename to init_connection
std::unique_ptr<duckdb::Connection> get_connection(
    const google::protobuf::Map<std::string, std::string> &request_config,
    const std::string &db_name, const std::shared_ptr<mdlog::MdLog> &logger) {
  std::string token = find_property(request_config, MD_PROP_TOKEN);

  duckdb::DBConfig config;
  config.SetOptionByName(MD_PROP_TOKEN, token);
  config.SetOptionByName("custom_user_agent", "fivetran");
  config.SetOptionByName("old_implicit_casting", true);
  config.SetOptionByName("motherduck_attach_mode", "single");

  duckdb::DuckDB db("md:" + db_name, &config);
  auto con = std::make_unique<duckdb::Connection>(db);
  auto result = con->Query("SELECT md_current_client_duckdb_id()");
  if (result->HasError()) {
    logger->warning("Could not retrieve the current duckdb ID: " +
                    result->GetError());
  } else {
    logger->set_duckdb_id(result->GetValue(0, 0).ToString());
  }
  return con;
}

const std::string
get_encryption_key(const std::string &filename,
                   const google::protobuf::Map<std::string, std::string> &keys,
                   ::fivetran_sdk::Encryption encryption) {
  if (encryption == ::fivetran_sdk::Encryption::NONE) {
    return "";
  }
  auto encryption_key_it = keys.find(filename);

  if (encryption_key_it == keys.end()) {
    throw std::invalid_argument("Missing encryption key for " + filename);
  }

  return encryption_key_it->second;
}

void validate_file(const std::string &file_path) {
  std::ifstream fs(file_path.c_str());
  if (fs.good()) {
    fs.close();
    return;
  }
  throw std::invalid_argument("File <" + file_path +
                              "> is missing or inaccessible");
}

void process_file(
    duckdb::Connection &con, IngestProperties &props,
    std::shared_ptr<mdlog::MdLog> &logger,
    const std::function<void(const std::string &view_name)> &process_view) {

  validate_file(props.filename);
  auto table = props.decryption_key.empty() ? read_unencrypted_csv(props)
                                            : read_encrypted_csv(props);

  auto batch_reader = std::make_shared<arrow::TableBatchReader>(*table);
  ArrowArrayStream arrow_array_stream;
  auto status =
      arrow::ExportRecordBatchReader(batch_reader, &arrow_array_stream);
  if (!status.ok()) {
    throw std::runtime_error(
        "Could not convert Arrow batch reader to an array stream for file <" +
        props.filename + ">: " + status.message());
  }
  logger->info("ArrowArrayStream created for file " + props.filename);

  duckdb_connection c_con = reinterpret_cast<duckdb_connection>(&con);
  duckdb_arrow_stream c_arrow_stream = (duckdb_arrow_stream)&arrow_array_stream;
  logger->info("duckdb_arrow_stream created for file " + props.filename);
  duckdb_arrow_scan(c_con, "arrow_view", c_arrow_stream);
  logger->info("duckdb_arrow_scan completed for file " + props.filename);

  process_view("\"localmem\".\"arrow_view\"");
  logger->info("view processed for file " + props.filename);

  arrow_array_stream.release(&arrow_array_stream);
}

grpc::Status DestinationSdkImpl::ConfigurationForm(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::ConfigurationFormRequest *request,
    ::fivetran_sdk::ConfigurationFormResponse *response) {

  response->set_schema_selection_supported(true);
  response->set_table_selection_supported(true);

  fivetran_sdk::FormField token_field;
  token_field.set_name(MD_PROP_TOKEN);
  token_field.set_label("Authentication Token");
  token_field.set_description(
      "Please get your authentication token from app.motherduck.com");
  token_field.set_text_field(fivetran_sdk::Password);
  token_field.set_required(true);

  response->add_fields()->CopyFrom(token_field);

  fivetran_sdk::FormField db_field;
  db_field.set_name(MD_PROP_DATABASE);
  db_field.set_label("Database Name");
  db_field.set_description("The database to work in");
  db_field.set_text_field(fivetran_sdk::PlainText);
  db_field.set_required(true);

  response->add_fields()->CopyFrom(db_field);

  fivetran_sdk::FormField block_size_field;
  block_size_field.set_name(MD_PROP_CSV_BLOCK_SIZE);
  block_size_field.set_label(
      "Maximum individual value size, in megabytes (default 1 MB)");
  block_size_field.set_description(
      "This field limits the maximum length of a single field value coming "
      "from the input source."
      "Must be a valid numeric value");
  block_size_field.set_text_field(fivetran_sdk::PlainText);
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

grpc::Status DestinationSdkImpl::DescribeTable(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::DescribeTableRequest *request,
    ::fivetran_sdk::DescribeTableResponse *response) {
  auto logger = std::make_shared<mdlog::MdLog>();
  try {
    logger->info("Endpoint <DescribeTable>: started");
    std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name, logger);
    auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

    table_def table_name{db_name, get_schema_name(request),
                         get_table_name(request)};

    if (!sql_generator->table_exists(*con, table_name)) {
      response->set_not_found(true);
      return ::grpc::Status(::grpc::StatusCode::OK, "");
    }

    logger->info("Endpoint <DescribeTable>: table exists; getting columns");
    auto duckdb_columns = sql_generator->describe_table(*con, table_name);
    logger->info("Endpoint <DescribeTable>: got columns");

    fivetran_sdk::Table *table = response->mutable_table();
    table->set_name(get_table_name(request));

    for (auto &col : duckdb_columns) {
      logger->info("Endpoint <DescribeTable>:   processing column " + col.name);
      fivetran_sdk::Column *ft_col = table->mutable_columns()->Add();
      ft_col->set_name(col.name);
      const auto fivetran_type = get_fivetran_type(col.type);
      logger->info("Endpoint <DescribeTable>:   column type = " +
                   std::to_string(fivetran_type));
      ft_col->set_type(fivetran_type);
      ft_col->set_primary_key(col.primary_key);
      if (fivetran_type == fivetran_sdk::DECIMAL) {
        ft_col->mutable_decimal()->set_precision(col.width);
        ft_col->mutable_decimal()->set_scale(col.scale);
      }
    }

  } catch (const std::exception &e) {
    logger->severe("DescribeTable endpoint failed for schema <" +
                   request->schema_name() + ">, table <" +
                   request->table_name() + ">:" + std::string(e.what()));
    response->set_failure(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  logger->info("Endpoint <DescribeTable>: ended");
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status DestinationSdkImpl::CreateTable(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::CreateTableRequest *request,
    ::fivetran_sdk::CreateTableResponse *response) {

  auto logger = std::make_shared<mdlog::MdLog>();
  try {
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
    response->set_failure(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status
DestinationSdkImpl::AlterTable(::grpc::ServerContext *context,
                               const ::fivetran_sdk::AlterTableRequest *request,
                               ::fivetran_sdk::AlterTableResponse *response) {
  auto logger = std::make_shared<mdlog::MdLog>();
  try {
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
    response->set_failure(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status
DestinationSdkImpl::Truncate(::grpc::ServerContext *context,
                             const ::fivetran_sdk::TruncateRequest *request,
                             ::fivetran_sdk::TruncateResponse *response) {

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

    logger->info("Endpoint <Truncate>: finished");
  } catch (const std::exception &e) {
    logger->severe("Truncate endpoint failed for schema <" +
                   request->schema_name() + ">, table <" +
                   request->table_name() + ">:" + std::string(e.what()));
    response->set_failure(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status
DestinationSdkImpl::WriteBatch(::grpc::ServerContext *context,
                               const ::fivetran_sdk::WriteBatchRequest *request,
                               ::fivetran_sdk::WriteBatchResponse *response) {

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

    // Use local memory by default to prevent Arrow-based VIEW from traveling
    // up to the cloud
    con->Query("ATTACH ':memory:' as localmem");
    con->Query("USE localmem");

    const auto cols = get_duckdb_columns(request->table().columns());
    std::vector<const column_def *> columns_pk;
    std::vector<const column_def *> columns_regular;
    find_primary_keys(cols, columns_pk, &columns_regular);

    if (columns_pk.empty()) {
      throw std::invalid_argument("No primary keys found");
    }

    // update file fields have to be read in as strings to allow
    // "unmodified_string"/"null_string". Replace (upsert) files have to be read
    // in as strings to allow "null_string".
    std::vector<std::string> column_names(cols.size());
    std::transform(cols.begin(), cols.end(), column_names.begin(),
                   [](const column_def &col) { return col.name; });

    for (auto &filename : request->replace_files()) {
      logger->info("Processing replace file " + filename);
      const auto decryption_key = get_encryption_key(
          filename, request->keys(), request->csv().encryption());

      IngestProperties props(filename, decryption_key, column_names,
                             request->csv().null_string(), csv_block_size);

      process_file(*con, props, logger, [&](const std::string &view_name) {
        sql_generator->upsert(*con, table_name, view_name, columns_pk,
                              columns_regular);
      });
    }
    for (auto &filename : request->update_files()) {
      logger->info("Processing update file " + filename);
      auto decryption_key = get_encryption_key(filename, request->keys(),
                                               request->csv().encryption());
      IngestProperties props(filename, decryption_key, column_names,
                             request->csv().null_string(), csv_block_size);

      process_file(*con, props, logger, [&](const std::string &view_name) {
        sql_generator->update_values(*con, table_name, view_name, columns_pk,
                                     columns_regular,
                                     request->csv().unmodified_string());
      });
    }
    for (auto &filename : request->delete_files()) {
      logger->info("Processing delete file " + filename);
      auto decryption_key = get_encryption_key(filename, request->keys(),
                                               request->csv().encryption());
      std::vector<std::string> empty;
      IngestProperties props(filename, decryption_key, empty,
                             request->csv().null_string(), csv_block_size);

      process_file(*con, props, logger, [&](const std::string &view_name) {
        sql_generator->delete_rows(*con, table_name, view_name, columns_pk);
      });
    }

  } catch (const std::exception &e) {

    auto const msg = "WriteBatch endpoint failed for schema <" +
                     request->schema_name() + ">, table <" +
                     request->table().name() + ">:" + std::string(e.what());
    logger->severe(msg);
    response->set_failure(msg);
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, msg);
  }

  logger->info("Endpoint <WriteBatch>: ended");
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
                         const ::fivetran_sdk::TestRequest *request,
                         ::fivetran_sdk::TestResponse *response) {

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
