#include <iostream>
#include <memory>
#include <string>

#include <arrow/c/bridge.h>
#include <grpcpp/grpcpp.h>

#include <csv_arrow_ingest.hpp>
#include <destination_sdk.grpc.pb.h>
#include <fivetran_duckdb_interop.hpp>
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
    duckdb_columns.push_back(
        column_def{col.name(), get_duckdb_type(col.type()), col.primary_key(),
                   col.decimal().precision(), col.decimal().scale()});
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
    const std::function<void(std::string view_name)> &process_view) {

  auto table = decryption_key.empty()
                   ? read_unencrypted_csv(filename, utf8_columns)
                   : read_encrypted_csv(filename, decryption_key, utf8_columns);

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
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status DestinationSdkImpl::DescribeTable(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::DescribeTableRequest *request,
    ::fivetran_sdk::DescribeTableResponse *response) {
  try {

    std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);
    table_def table_name{db_name, get_schema_name(request),
                         get_table_name(request)};

    if (!table_exists(*con, table_name)) {
      response->set_not_found(true);
      return ::grpc::Status(::grpc::StatusCode::OK, "");
    }

    auto duckdb_columns = describe_table(*con, table_name);

    fivetran_sdk::Table *table = response->mutable_table();
    table->set_name(get_table_name(request));

    for (auto &col : duckdb_columns) {
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
        find_property(request->configuration(), MD_PROP_DATABASE);
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);
    const table_def table{db_name, schema_name, request->table().name()};

    if (!schema_exists(*con, db_name, schema_name)) {
      create_schema(*con, db_name, schema_name);
    }

    create_table(*con, table, get_duckdb_columns(request->table().columns()));
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
        find_property(request->configuration(), MD_PROP_DATABASE);
    table_def table_name{db_name, get_schema_name(request),
                         request->table().name()};

    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);

    alter_table(*con, table_name,
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
      find_property(request->configuration(), MD_PROP_DATABASE);
  table_def table_name{db_name, get_schema_name(request),
                       get_table_name(request)};
  std::unique_ptr<duckdb::Connection> con =
      get_connection(request->configuration(), db_name);

  truncate_table(*con, table_name);
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status
DestinationSdkImpl::WriteBatch(::grpc::ServerContext *context,
                               const ::fivetran_sdk::WriteBatchRequest *request,
                               ::fivetran_sdk::WriteBatchResponse *response) {

  try {
    auto schema_name = get_schema_name(request);

    const std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    table_def table_name{db_name, get_schema_name(request),
                         request->table().name()};
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);

    // Use local memory by default to prevent Arrow-based VIEW from traveling
    // up to the cloud
    con->Query("ATTACH ':memory:' as localmem");
    con->Query("USE localmem");

    const auto cols = get_duckdb_columns(request->table().columns());
    std::vector<const column_def *> columns_pk;
    std::vector<const column_def *> columns_regular;
    for (auto &col : cols) {
      if (col.primary_key) {
        columns_pk.push_back(&col);
      } else {
        columns_regular.push_back(&col);
      }
    }

    std::vector<std::string> empty;
    for (auto &filename : request->replace_files()) {
      const auto decryption_key = get_encryption_key(
          filename, request->keys(), request->csv().encryption());

      process_file(*con, filename, decryption_key, empty,
                   [&](const std::string view_name) {
                     upsert(*con, table_name, view_name, columns_pk,
                            columns_regular);
                   });
    }
    for (auto &filename : request->update_files()) {

      auto decryption_key = get_encryption_key(filename, request->keys(),
                                               request->csv().encryption());
      // update file fields have to be read in as strings to allow
      // "unmodified_string" processing
      std::vector<std::string> column_names(cols.size());
      std::transform(cols.begin(), cols.end(), column_names.begin(),
                     [](const column_def &col) { return col.name; });

      process_file(*con, filename, decryption_key, column_names,
                   [&](const std::string view_name) {
                     update_values(*con, table_name, view_name, columns_pk,
                                   columns_regular,
                                   request->csv().unmodified_string());
                   });
    }
    for (auto &filename : request->delete_files()) {
      auto decryption_key = get_encryption_key(filename, request->keys(),
                                               request->csv().encryption());
      process_file(*con, filename, decryption_key, empty,
                   [&](const std::string view_name) {
                     delete_rows(*con, table_name, view_name, columns_pk);
                   });
    }

  } catch (const std::exception &e) {
    response->set_failure(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status
DestinationSdkImpl::Test(::grpc::ServerContext *context,
                         const ::fivetran_sdk::TestRequest *request,
                         ::fivetran_sdk::TestResponse *response) {

  try {
    std::string db_name =
        find_property(request->configuration(), MD_PROP_DATABASE);
    std::unique_ptr<duckdb::Connection> con =
        get_connection(request->configuration(), db_name);
    check_connection(*con);
  } catch (const std::exception &e) {
    response->set_failure(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  return ::grpc::Status(::grpc::StatusCode::OK, "");
}
