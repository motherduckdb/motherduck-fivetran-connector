#include <iostream>
#include <memory>
#include <string>

#include <arrow/c/bridge.h>
#include <grpcpp/grpcpp.h>

#include <sql_generator.hpp>
#include <destination_sdk.grpc.pb.h>
#include <motherduck_destination_server.hpp>

#include <fivetran_duckdb_interop.hpp>
#include <csv_arrow_ingest.hpp>

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
  for (auto col : fivetran_columns) {
    // todo: if not decimal? (hasDecimal())
    duckdb_columns.push_back(
        column_def{col.name(), get_duckdb_type(col.type()), col.primary_key(),
                   col.decimal().precision(), col.decimal().scale()});
  }
  return duckdb_columns;
}

std::unique_ptr<duckdb::Connection> get_connection(
    const google::protobuf::Map<std::string, std::string> &request_config,
    const std::string &db_name) {
  std::string token = find_property(request_config, "motherduck_token");

  std::unordered_map<std::string, std::string> props{
      {"motherduck_token", token}, {"custom_user_agent", "fivetran"}};
  duckdb::DBConfig config(props, false);
  duckdb::DuckDB db("md:" + db_name, &config);
  return std::make_unique<duckdb::Connection>(db);
}


const std::string *
get_encryption_key(const std::string &filename,
                   const google::protobuf::Map<std::string, std::string> &keys,
                   ::fivetran_sdk::Encryption encryption) {
  if (encryption == ::fivetran_sdk::Encryption::NONE) {
    return nullptr;
  }
  auto encryption_key_it = keys.find(filename);

  if (encryption_key_it == keys.end()) {
    throw std::invalid_argument("Missing encryption key for " + filename);
  }

  return &encryption_key_it->second;
}

std::vector<std::string> get_primary_keys(
    const google::protobuf::RepeatedPtrField<fivetran_sdk::Column> &columns) {
  std::vector<std::string> primary_keys;
  for (auto &col : columns) {
    if (col.primary_key()) {
      primary_keys.push_back(col.name());
    }
  }
  return primary_keys;
}

void process_file(
    duckdb::Connection &con, const std::string &filename,
    const std::string *decryption_key,
    std::vector<std::string>* utf8_columns,
    const std::function<void(std::string view_name)> &process_view) {

  auto table =
      decryption_key == nullptr
          ? ReadUnencryptedCsv(filename, utf8_columns)
          : ReadEncryptedCsv(filename, decryption_key, utf8_columns);

  auto batch_reader = std::make_shared<arrow::TableBatchReader>(*table);
  ArrowArrayStream arrow_array_stream;
  auto status =
      arrow::ExportRecordBatchReader(batch_reader, &arrow_array_stream);
  if (!status.ok()) {
    throw std::runtime_error(
        "Could not convert Arrow batch reader to an array stream: " +
        status.message());
  }

  duckdb_connection c_con = (duckdb_connection)&con;
  duckdb_arrow_stream c_arrow_stream = (duckdb_arrow_stream)&arrow_array_stream;
  duckdb_arrow_scan(c_con, "arrow_view", c_arrow_stream);

  process_view("localmem.arrow_view");

  arrow_array_stream.release(&arrow_array_stream);
}

grpc::Status DestinationSdkImpl::ConfigurationForm(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::ConfigurationFormRequest *request,
    ::fivetran_sdk::ConfigurationFormResponse *response) {

  response->set_schema_selection_supported(true);
  response->set_table_selection_supported(true);

  fivetran_sdk::FormField token_field;
  token_field.set_name("motherduck_token");
  token_field.set_label("Authentication Token");
  token_field.set_description(
      "Please get your authentication token from app.motherduck.com");
  token_field.set_text_field(fivetran_sdk::Password);
  token_field.set_required(true);

  fivetran_sdk::FormField db_field;
  db_field.set_name("motherduck_database");
  db_field.set_label("Database Name");
  db_field.set_description("The database to work in");
  db_field.set_text_field(fivetran_sdk::PlainText);
  db_field.set_required(true);

  response->add_fields()->CopyFrom(token_field);
  response->add_fields()->CopyFrom(db_field);
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status DestinationSdkImpl::DescribeTable(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::DescribeTableRequest *request,
    ::fivetran_sdk::DescribeTableResponse *response) {
  try {

    std::string db_name =
        find_property(request->configuration(), "motherduck_database");
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);

    if (!table_exists(*con, db_name, get_schema_name(request),
                      get_table_name(request))) {
      response->set_not_found(true);
      return ::grpc::Status(::grpc::StatusCode::OK, "");
    }

    auto duckdb_columns = describe_table(
        *con, db_name, get_schema_name(request), get_table_name(request));

    fivetran_sdk::Table *table = response->mutable_table();
    table->set_name(get_table_name(request));

    for (auto col : duckdb_columns) {
      fivetran_sdk::Column *ft_col = table->mutable_columns()->Add();
      ft_col->set_name(col.name);
      ft_col->set_type(get_fivetran_type(col.type));
      ft_col->set_primary_key(col.primary_key);
    }

  } catch (const std::exception &e) {
    response->set_failure(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status DestinationSdkImpl::CreateTable(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::CreateTableRequest *request,
    ::fivetran_sdk::CreateTableResponse *response) {

  try {
    auto schema_name = get_schema_name(request);

    std::string db_name =
        find_property(request->configuration(), "motherduck_database");
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);

    if (!schema_exists(*con, db_name, schema_name)) {
      create_schema(*con, db_name, schema_name);
    }

    create_table(*con, db_name, schema_name, request->table().name(),
                 get_duckdb_columns(request->table().columns()));
    response->set_success(true);
  } catch (const std::exception &e) {
    response->set_failure(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status
DestinationSdkImpl::AlterTable(::grpc::ServerContext *context,
                               const ::fivetran_sdk::AlterTableRequest *request,
                               ::fivetran_sdk::AlterTableResponse *response) {
  try {
    std::string db_name =
        find_property(request->configuration(), "motherduck_database");
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);

    alter_table(*con, db_name, get_schema_name(request),
                request->table().name(),
                get_duckdb_columns(request->table().columns()));
    response->set_success(true);
  } catch (const std::exception &e) {
    response->set_failure(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status
DestinationSdkImpl::Truncate(::grpc::ServerContext *context,
                             const ::fivetran_sdk::TruncateRequest *request,
                             ::fivetran_sdk::TruncateResponse *response) {
  std::string db_name =
      find_property(request->configuration(), "motherduck_database");
  std::unique_ptr<duckdb::Connection> con =
      get_connection(request->configuration(), db_name);

  truncate_table(*con, db_name, get_schema_name(request),
                 get_table_name(request));
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status
DestinationSdkImpl::WriteBatch(::grpc::ServerContext *context,
                               const ::fivetran_sdk::WriteBatchRequest *request,
                               ::fivetran_sdk::WriteBatchResponse *response) {

  try {
    auto schema_name = get_schema_name(request);

    std::string db_name =
        find_property(request->configuration(), "motherduck_database");
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);

    // Use local memory by default to prevent Arrow-based VIEW from traveling
    // up to the cloud
    con->Query("ATTACH ':memory:' as localmem");
    con->Query("USE localmem");

    const auto primary_keys = get_primary_keys(request->table().columns());
    const auto cols = get_duckdb_columns(request->table().columns());
    std::vector<std::string> column_names(cols.size());
    std::transform(cols.begin(), cols.end(), column_names.begin(), [](const column_def& col) { return col.name; });

    for (auto &filename : request->replace_files()) {
      auto decryption_key = get_encryption_key(filename, request->keys(),
                                               request->csv().encryption());
      process_file(*con, filename, decryption_key, nullptr,
                   [&con, &db_name, &request, &primary_keys, &cols,
                    &schema_name](const std::string view_name) {
                     upsert(*con, db_name, schema_name, request->table().name(),
                            view_name, primary_keys, cols);
                   });
    }
    for (auto &filename : request->update_files()) {

      auto decryption_key = get_encryption_key(filename, request->keys(),
                                               request->csv().encryption());
      process_file(*con, filename, decryption_key, &column_names,
                   [&con, &db_name, &request, &primary_keys, &cols,
                    &schema_name](const std::string view_name) {
                     update_values(*con, db_name, schema_name,
                                   request->table().name(), view_name,
                                   primary_keys, cols,
                                   request->csv().unmodified_string());
                   });
    }
    for (auto &filename : request->delete_files()) {
      auto decryption_key = get_encryption_key(filename, request->keys(),
                                               request->csv().encryption());
      process_file(*con, filename, decryption_key, nullptr,
                   [&con, &db_name, &request, &primary_keys,
                    &schema_name](const std::string view_name) {
                     delete_rows(*con, db_name, schema_name,
                                 request->table().name(), view_name,
                                 primary_keys);
                   });
    }

  } catch (const std::exception &e) {
    response->set_failure(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status DestinationSdkImpl::Test(::grpc::ServerContext *context,
                                const ::fivetran_sdk::TestRequest *request,
                                ::fivetran_sdk::TestResponse *response) {

  try {
    std::string db_name =
        find_property(request->configuration(), "motherduck_database");
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);
    check_connection(*con);
  } catch (const std::exception &e) {
    response->set_failure(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  return ::grpc::Status(::grpc::StatusCode::OK, "");
}
