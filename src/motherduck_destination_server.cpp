#include <iostream>
#include <memory>
#include <string>
#include <cstdlib>

#include <arrow/c/bridge.h>
#include <grpcpp/grpcpp.h>

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
    duckdb_columns.push_back(column_def{col.name(), ddbtype, col.primary_key(),
                                        col.decimal().precision(),
                                        col.decimal().scale()});
  }
  return duckdb_columns;
}

std::unique_ptr<duckdb::Connection> get_connection(
    const google::protobuf::Map<std::string, std::string> &request_config,
    const std::string &db_name) {
  std::string token = find_property(request_config, MD_PROP_TOKEN);

  std::unordered_map<std::string, std::string> props{
      {MD_PROP_TOKEN, token}, {"custom_user_agent", "fivetran"}};
  duckdb::DBConfig config(props, false);
  duckdb::DuckDB db("md:" + db_name, &config);
  return std::make_unique<duckdb::Connection>(db);
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

void process_file(
    duckdb::Connection &con, const std::string &filename,
    const std::string &decryption_key, std::vector<std::string> &utf8_columns,
    const std::string &null_value,
    const std::function<void(const std::string &view_name)> &process_view) {

  auto table = decryption_key.empty()
                   ? read_unencrypted_csv(filename, utf8_columns, null_value)
                   : read_encrypted_csv(filename, decryption_key, utf8_columns,
                                        null_value);

  auto batch_reader = std::make_shared<arrow::TableBatchReader>(*table);
  ArrowArrayStream arrow_array_stream;
  auto status =
      arrow::ExportRecordBatchReader(batch_reader, &arrow_array_stream);
  if (!status.ok()) {
    throw std::runtime_error(
        "Could not convert Arrow batch reader to an array stream for file <" +
        filename + ">: " + status.message());
  }

  duckdb_connection c_con = reinterpret_cast<duckdb_connection>(&con);
  duckdb_arrow_stream c_arrow_stream = (duckdb_arrow_stream)&arrow_array_stream;
  duckdb_arrow_scan(c_con, "arrow_view", c_arrow_stream);

  process_view("localmem.arrow_view");

  arrow_array_stream.release(&arrow_array_stream);
}

void find_primary_keys(
    const std::vector<column_def> &cols,
    std::vector<const column_def *> &columns_pk,
    std::vector<const column_def *> *columns_regular = nullptr) {
  for (auto &col : cols) {
    if (col.primary_key) {
      columns_pk.push_back(&col);
    } else if (columns_regular != nullptr) {
      columns_regular->push_back(&col);
    }
  }
}

bool disable_host_check(const std::string &host) {
  const auto env_disable_host_check = std::getenv("motherduck_disable_host_check");
  if (env_disable_host_check == nullptr) {
    return false;
  }
  const auto value = std::string(env_disable_host_check);
  return host == "localhost" || host == "127.0.0.1" || host == "::1" ||
          value == "true" || value == "1";
}

std::shared_ptr<grpc::Channel> CreateChannelFromConfig(const std::string &host, int64_t port, bool use_tls) {
  grpc::ChannelArguments channel_arguments;

  // Prevent AWS NLB from closing the TCP connection after 350 seconds by closing it first after 4 minutes.
  channel_arguments.SetInt("grpc.client_idle_timeout_ms", 240000);
  // When a query is active, send a keepalive every minute to keep the connection active.
  channel_arguments.SetInt("grpc.keepalive_time_ms", 60000);
  // Timeout consistent with default.
  channel_arguments.SetInt("grpc.keepalive_timeout_ms", 20000);

  // If grpc.keepalive_time_ms is on, then grpc.http2.max_pings_without_data has to be modified
  // (the default is only 2, so only 1 additional keepalive is sent after the initial query execution starts).
  channel_arguments.SetInt("grpc.http2.max_pings_without_data", 0);

  std::shared_ptr<grpc::ChannelCredentials> channel_credentials;
  if (use_tls) {
    grpc::experimental::TlsChannelCredentialsOptions tls_options;
    tls_options.set_verify_server_certs(false); // TODO: enable server verification before public release
    if (disable_host_check(host)) {
      // dev mode: disable all host validation and cert checks
      std::cout << "DISABLING HOST CHECK" << std::endl;
      tls_options.set_check_call_host(false);
      tls_options.set_certificate_verifier(std::make_shared<grpc::experimental::NoOpCertificateVerifier>());
    } else {
      std::cout << "NOT DISABLING HOST CHECK" << std::endl;

    }
    channel_credentials = grpc::experimental::TlsCredentials(tls_options);
  } else {
    channel_credentials = grpc::InsecureChannelCredentials();
  }

  return grpc::CreateCustomChannel(host + ":" + std::to_string(port), channel_credentials, channel_arguments);
}


DestinationSdkImpl::DestinationSdkImpl() {

  const char* logging_host = std::getenv("motherduck_host");
  if (logging_host == nullptr) {
    logging_host = "api.motherduck.com";
  }
  std::cout << "logging going to " << logging_host << std::endl;
  auto logging_port = 443;
  auto use_tls = true;
  loggingSinkClient = std::shared_ptr<logging_sink::LoggingSink::Stub>(std::move(
      logging_sink::LoggingSink::NewStub(CreateChannelFromConfig(logging_host, logging_port, use_tls))));

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

  fivetran_sdk::FormField db_field;
  db_field.set_name(MD_PROP_DATABASE);
  db_field.set_label("Database Name");
  db_field.set_description("The database to work in");
  db_field.set_text_field(fivetran_sdk::PlainText);
  db_field.set_required(true);

  response->add_fields()->CopyFrom(token_field);
  response->add_fields()->CopyFrom(db_field);

  auto test = response->add_tests();
  test->set_name(CONFIG_TEST_NAME_AUTHENTICATE);
  test->set_label("Test Authentication");
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status DestinationSdkImpl::DescribeTable(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::DescribeTableRequest *request,
    ::fivetran_sdk::DescribeTableResponse *response) {
  // TODO: refactor -- token is retrieved twice now; this needs error handling
  const std::string token = find_property(request->configuration(), MD_PROP_TOKEN);
  auto logger = std::make_shared<mdlog::MdLog>(token, loggingSinkClient);
  auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

  try {
    logger->info("Endpoint <DescribeTable>: started");
    std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    logger->info("Endpoint <DescribeTable>: found database name <" + db_name + ">");
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);
    logger->info("Endpoint <DescribeTable>: got database connection");
    table_def table_name{db_name, get_schema_name(request),
                         get_table_name(request)};

    if (!sql_generator->table_exists(*con, table_name)) {
      logger->info("Endpoint <DescribeTable>: table does not exist; returning not found");
      response->set_not_found(true);
      return ::grpc::Status(::grpc::StatusCode::OK, "");
    }

    auto duckdb_columns = sql_generator->describe_table(*con, table_name);

    fivetran_sdk::Table *table = response->mutable_table();
    table->set_name(get_table_name(request));

    logger->info("Endpoint <DescribeTable>: before enumerating columns");
    for (auto &col : duckdb_columns) {
      fivetran_sdk::Column *ft_col = table->mutable_columns()->Add();
      logger->info("Endpoint <DescribeTable>: column <" + col.name + ">; duckdb type <" + LogicalTypeIdToString(col.type) + ">");
      ft_col->set_name(col.name);
      auto fivetran_type = get_fivetran_type(col.type);
      logger->info("Endpoint <DescribeTable>: column <" + col.name + ">; fivetran type <" + DataType_Name(fivetran_type) + ">");
      ft_col->set_type(fivetran_type);
      ft_col->set_primary_key(col.primary_key);
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

  // TODO: refactor -- token is retrieved twice now; this needs error handling
  const std::string token = find_property(request->configuration(), MD_PROP_TOKEN);
  auto logger = std::make_shared<mdlog::MdLog>(token, loggingSinkClient);
  auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

  try {
    auto schema_name = get_schema_name(request);

    std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);
    const table_def table{db_name, schema_name, request->table().name()};

    if (!sql_generator->schema_exists(*con, db_name, schema_name)) {
      sql_generator->create_schema(*con, db_name, schema_name);
    }

    std::vector<const column_def *> columns_pk;
    const auto cols = get_duckdb_columns(request->table().columns());
    find_primary_keys(cols, columns_pk);
    sql_generator->create_table(*con, table, columns_pk, cols);
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
  // TODO: refactor -- token is retrieved twice now; this needs error handling
  const std::string token = find_property(request->configuration(), MD_PROP_TOKEN);
  auto logger = std::make_shared<mdlog::MdLog>(token, loggingSinkClient);
  auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

  try {
    std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    table_def table_name{db_name, get_schema_name(request),
                         request->table().name()};

    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);

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
  // TODO: refactor -- token is retrieved twice now; this needs error handling
  const std::string token = find_property(request->configuration(), MD_PROP_TOKEN);
  auto logger = std::make_shared<mdlog::MdLog>(token, loggingSinkClient);
  auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

  try {
    logger->info("Endpoint <Truncate>: started");
    std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    logger->info("Endpoint <Truncate>: found database name <" + db_name + ">");
    table_def table_name{db_name, get_schema_name(request),
                         get_table_name(request)};
    if (request->synced_column().empty()) {
      throw std::invalid_argument("Synced column is required");
    }

    logger->info("Endpoint <Truncate>: found synced column <" + request->synced_column() + ">");
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);

    logger->info("Endpoint <Truncate>: got database connection");
    if (sql_generator->table_exists(*con, table_name)) {
      logger->info("Endpoint <Truncate>: schema <" + table_name.schema_name + ">, table <" + table_name.table_name + "> exists");
      std::chrono::nanoseconds delete_before_ts =
          std::chrono::seconds(request->utc_delete_before().seconds()) +
          std::chrono::nanoseconds(request->utc_delete_before().nanos());
      logger->info("Endpoint <Truncate>: delete_before_ts = <" + std::to_string(delete_before_ts.count()) + ">");
      sql_generator->truncate_table(*con, table_name, request->synced_column(),
                     delete_before_ts, request->soft().deleted_column());
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

  // TODO: refactor -- token is retrieved twice now; this needs error handling
  const std::string token = find_property(request->configuration(), MD_PROP_TOKEN);
  auto logger = std::make_shared<mdlog::MdLog>(token, loggingSinkClient);
  auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

  try {
    logger->info("Endpoint <WriteBatch>: started");
    auto schema_name = get_schema_name(request);

    const std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    logger->info("Endpoint <WriteBatch>: found database name <" + db_name + ">");
    table_def table_name{db_name, get_schema_name(request),
                         request->table().name()};
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);
    logger->info("Endpoint <WriteBatch>: got database connection");

    // Use local memory by default to prevent Arrow-based VIEW from traveling
    // up to the cloud
    con->Query("ATTACH ':memory:' as localmem");
    con->Query("USE localmem");
    logger->info("Endpoint <WriteBatch>: attached and used local memory db");

    const auto cols = get_duckdb_columns(request->table().columns());
    std::vector<const column_def *> columns_pk;
    std::vector<const column_def *> columns_regular;
    find_primary_keys(cols, columns_pk, &columns_regular);

    if (columns_pk.empty()) {
      throw std::invalid_argument("No primary keys found");
    }
    logger->info("Endpoint <WriteBatch>: got " + std::to_string(columns_pk.size()) + " primary keys");

    // update file fields have to be read in as strings to allow
    // "unmodified_string"/"null_string". Replace (upsert) files have to be read
    // in as strings to allow "null_string".
    std::vector<std::string> column_names(cols.size());
    std::transform(cols.begin(), cols.end(), column_names.begin(),
                   [](const column_def &col) { return col.name; });

    for (auto &filename : request->replace_files()) {
      logger->info("Endpoint <WriteBatch>: processing replace file " + filename);
      const auto decryption_key = get_encryption_key(
          filename, request->keys(), request->csv().encryption());

      logger->info("Endpoint <WriteBatch>: got replace file decryption key");
      process_file(
          *con, filename, decryption_key, column_names,
          request->csv().null_string(), [&](const std::string &view_name) {
              sql_generator->upsert(*con, table_name, view_name, columns_pk, columns_regular);
          });
      logger->info("Endpoint <WriteBatch>: finished processing replace file " + filename);
    }
    for (auto &filename : request->update_files()) {
      logger->info("Endpoint <WriteBatch>: processing update file " + filename);
      auto decryption_key = get_encryption_key(filename, request->keys(),
                                               request->csv().encryption());
      logger->info("Endpoint <WriteBatch>: got update file decryption key");
      process_file(
          *con, filename, decryption_key, column_names,
          request->csv().null_string(), [&](const std::string &view_name) {
              sql_generator->update_values(*con, table_name, view_name, columns_pk,
                          columns_regular, request->csv().unmodified_string());
          });
      logger->info("Endpoint <WriteBatch>: finished processing update file " + filename);
    }
    for (auto &filename : request->delete_files()) {
      logger->info("Endpoint <WriteBatch>: processing delete file " + filename);
      auto decryption_key = get_encryption_key(filename, request->keys(),
                                               request->csv().encryption());
      logger->info("Endpoint <WriteBatch>: got delete file decryption key");
      std::vector<std::string> empty;
      process_file(*con, filename, decryption_key, empty,
                   request->csv().null_string(),
                   [&](const std::string &view_name) {
                       sql_generator->delete_rows(*con, table_name, view_name, columns_pk);
                   });
      logger->info("Endpoint <WriteBatch>: finished processing delete file " + filename);
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

grpc::Status
DestinationSdkImpl::Test(::grpc::ServerContext *context,
                         const ::fivetran_sdk::TestRequest *request,
                         ::fivetran_sdk::TestResponse *response) {

  // TODO: refactor -- token is retrieved twice now; this needs error handling
  const std::string token = find_property(request->configuration(), MD_PROP_TOKEN);
  auto logger = std::make_shared<mdlog::MdLog>(token, loggingSinkClient);
  auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

  logger->warning("ELENA TRYING SOMETHING");
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
        get_connection(request->configuration(), db_name);

    if (request->name() == CONFIG_TEST_NAME_AUTHENTICATE) {
      sql_generator->check_connection(*con);
      response->set_success(true);
    } else {
      auto const msg = "Unknown test requested: <" + request->name() + ">";
      logger->severe(msg);
      response->set_success(false);
      response->set_failure(msg);
      return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, msg);
    }
    response->set_success(true);
  } catch (const std::exception &e) {
    auto msg = "Authentication test for database <" + db_name +
               "> failed: " + std::string(e.what());
    response->set_success(false);
    response->set_failure(msg);
    // grpc call succeeded; the response reflects config test failure
    return ::grpc::Status(::grpc::StatusCode::OK, msg);
  }

  return ::grpc::Status(::grpc::StatusCode::OK, "");
}
