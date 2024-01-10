#include <iostream>
#include <memory>
#include <string>

#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/util/compression.h>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include <openssl/evp.h>

#include "../includes/sql_generator.hpp"
#include "destination_sdk.grpc.pb.h"
#include "motherduck_destination_server.hpp"

using duckdb::Connection;
using duckdb::DBConfig;
using duckdb::DuckDB;
using duckdb::KeywordHelper;
using duckdb::LogicalTypeId;
using duckdb::MaterializedQueryResult;
using duckdb::PreparedStatement;
using duckdb::QueryResult;
using fivetran_sdk::Destination;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

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

fivetran_sdk::DataType get_fivetran_type(const LogicalTypeId &duckdb_type) {
  switch (duckdb_type) {
  case LogicalTypeId::BOOLEAN:
    return fivetran_sdk::BOOLEAN;
  case LogicalTypeId::SMALLINT:
    return fivetran_sdk::SHORT;
  case LogicalTypeId::INTEGER:
    return fivetran_sdk::INT;
  case LogicalTypeId::BIGINT:
    return fivetran_sdk::LONG;
  case LogicalTypeId::FLOAT:
    return fivetran_sdk::FLOAT;
  case LogicalTypeId::DOUBLE:
    return fivetran_sdk::DOUBLE;
  case LogicalTypeId::DATE:
    return fivetran_sdk::NAIVE_DATE;
  case LogicalTypeId::TIMESTAMP:
    return fivetran_sdk::UTC_DATETIME; // TBD: this is pretty definitely
                                       // wrong, and should naive time be
                                       // returned for any reason?
  case LogicalTypeId::DECIMAL:
    return fivetran_sdk::DECIMAL;
  case LogicalTypeId::BIT:
    return fivetran_sdk::BINARY; // TBD: double check if correct
  case LogicalTypeId::VARCHAR:
    return fivetran_sdk::STRING;
  case LogicalTypeId::STRUCT:
    return fivetran_sdk::JSON;
  default:
    return fivetran_sdk::UNSPECIFIED;
  }
}

LogicalTypeId get_duckdb_type(const fivetran_sdk::DataType &fivetranType) {
  switch (fivetranType) {
  case fivetran_sdk::BOOLEAN:
    return LogicalTypeId::BOOLEAN;
  case fivetran_sdk::SHORT:
    return LogicalTypeId::SMALLINT;
  case fivetran_sdk::INT:
    return LogicalTypeId::INTEGER;
  case fivetran_sdk::LONG:
    return LogicalTypeId::BIGINT;
  case fivetran_sdk::FLOAT:
    return LogicalTypeId::FLOAT;
  case fivetran_sdk::DOUBLE:
    return LogicalTypeId::DOUBLE;
  case fivetran_sdk::NAIVE_DATE:
    return LogicalTypeId::DATE;
  case fivetran_sdk::NAIVE_DATETIME: // TBD: what kind is this?
    return LogicalTypeId::TIMESTAMP; // TBD: this is pretty definitely wrong
  case fivetran_sdk::UTC_DATETIME:
    return LogicalTypeId::TIMESTAMP; // TBD: this is pretty definitely wrong
  case fivetran_sdk::DECIMAL:
    return LogicalTypeId::DECIMAL;
  case fivetran_sdk::BINARY:
    return LogicalTypeId::BIT; // TBD: double check if correct
  case fivetran_sdk::STRING:
    return LogicalTypeId::VARCHAR;
  case fivetran_sdk::JSON:
    return LogicalTypeId::STRUCT;
  default:
    return LogicalTypeId::INVALID;
  }
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

std::unique_ptr<Connection> get_connection(
    const google::protobuf::Map<std::string, std::string> &request_config,
    const std::string &db_name) {
  std::string token = find_property(request_config, "motherduck_token");

  std::unordered_map<std::string, std::string> props{
      {"motherduck_token", token}};
  DBConfig config(props, false);
  DuckDB db("md:" + db_name, &config);
  return std::make_unique<Connection>(db);
}

std::vector<unsigned char> decrypt_file(const std::string &filename,
                                        const unsigned char *decryption_key) {

  std::ifstream file(filename, std::ios::binary);
  std::vector<unsigned char> iv(16);
  file.read(reinterpret_cast<char *>(iv.data()), iv.size());

  std::vector<unsigned char> encrypted_data(
      std::istreambuf_iterator<char>{file}, {});

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (!ctx) {
    throw std::runtime_error("Could not initialize decryption context");
  }
  if (1 != EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), NULL, decryption_key,
                              iv.data()))
    throw std::runtime_error("Could not decrypt file " + filename);
  int len;
  std::vector<unsigned char> plaintext(encrypted_data.size());
  if (1 != EVP_DecryptUpdate(ctx, plaintext.data(), &len, encrypted_data.data(),
                             encrypted_data.size()))
    throw std::runtime_error("Could not decrypt UPDATE file " + filename);
  int plaintext_len = len;
  if (1 != EVP_DecryptFinal_ex(ctx, plaintext.data() + len, &len))
    throw std::runtime_error("Could not decrypt FINAL file " + filename);
  plaintext_len += len;
  EVP_CIPHER_CTX_free(ctx);

  plaintext.resize(plaintext_len);

  return plaintext;
}

std::shared_ptr<arrow::Table>
ReadEncryptedCsv(const std::string &filename, const std::string *decryption_key,
                 arrow::csv::ConvertOptions &convert_options) {

  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();

  std::vector<unsigned char> plaintext = decrypt_file(
      filename,
      reinterpret_cast<const unsigned char *>(decryption_key->c_str()));
  auto buffer = std::make_shared<arrow::Buffer>(
      reinterpret_cast<const uint8_t *>(plaintext.data()), plaintext.size());
  auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);

  arrow::Compression::type compression_type = arrow::Compression::ZSTD;
  auto maybe_codec = arrow::util::Codec::Create(compression_type);
  if (!maybe_codec.ok()) {
    throw std::runtime_error(
        "Could not create codec from ZSTD compression type: " +
        maybe_codec.status().message());
  }
  auto codec = std::move(maybe_codec.ValueOrDie());
  auto maybe_compressed_input_stream =
      arrow::io::CompressedInputStream::Make(codec.get(), buffer_reader);
  if (!maybe_compressed_input_stream.ok()) {
    throw std::runtime_error("Could not input stream from compressed buffer: " +
                             maybe_compressed_input_stream.status().message());
  }
  auto compressed_input_stream =
      std::move(maybe_compressed_input_stream.ValueOrDie());
  auto maybe_table_reader = arrow::csv::TableReader::Make(
      arrow::io::default_io_context(), std::move(compressed_input_stream),
      read_options, parse_options, convert_options);
  if (!maybe_table_reader.ok()) {
    throw std::runtime_error(
        "Could not create table reader from decrypted file: " +
        maybe_table_reader.status().message());
  }
  auto table_reader = std::move(maybe_table_reader.ValueOrDie());

  auto maybe_table = table_reader->Read();
  if (!maybe_table.ok()) {
    throw std::runtime_error("Could not read CSV <" + filename +
                             ">: " + maybe_table.status().message());
  }
  auto table = std::move(maybe_table.ValueOrDie());

  return table;
}

std::shared_ptr<arrow::Table>
ReadUnencryptedCsv(const std::string &filename,
                   arrow::csv::ConvertOptions &convert_options) {

  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();

  auto maybe_file =
      arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  if (!maybe_file.ok()) {
    throw std::runtime_error("Could not open uncompressed file <" + filename +
                             ">: " + maybe_file.status().message());
  }
  auto plaintext_input_stream = std::move(maybe_file.ValueOrDie());
  auto maybe_table_reader = arrow::csv::TableReader::Make(
      arrow::io::default_io_context(), std::move(plaintext_input_stream),
      read_options, parse_options, convert_options);
  if (!maybe_table_reader.ok()) {
    throw std::runtime_error(
        "Could not create table reader from plaintext file: " +
        maybe_table_reader.status().message());
  }
  auto table_reader = std::move(maybe_table_reader.ValueOrDie());

  auto maybe_table = table_reader->Read();
  if (!maybe_table.ok()) {
    throw std::runtime_error("Could not read CSV <" + filename +
                             ">: " + maybe_table.status().message());
  }
  auto table = std::move(maybe_table.ValueOrDie());

  return table;
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
    Connection &con, const std::string &filename,
    const std::string *decryption_key,
    arrow::csv::ConvertOptions &convert_options,
    const std::function<void(std::string view_name)> &process_view) {

  auto table =
      decryption_key == nullptr
          ? ReadUnencryptedCsv(filename, convert_options)
          : ReadEncryptedCsv(filename, decryption_key, convert_options);

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

Status DestinationSdkImpl::ConfigurationForm(
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

Status DestinationSdkImpl::DescribeTable(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::DescribeTableRequest *request,
    ::fivetran_sdk::DescribeTableResponse *response) {
  try {

    std::string db_name =
        find_property(request->configuration(), "motherduck_database");
    std::unique_ptr<Connection> con =
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

Status DestinationSdkImpl::CreateTable(
    ::grpc::ServerContext *context,
    const ::fivetran_sdk::CreateTableRequest *request,
    ::fivetran_sdk::CreateTableResponse *response) {

  try {
    auto schema_name = get_schema_name(request);

    std::string db_name =
        find_property(request->configuration(), "motherduck_database");
    std::unique_ptr<Connection> con =
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

Status
DestinationSdkImpl::AlterTable(::grpc::ServerContext *context,
                               const ::fivetran_sdk::AlterTableRequest *request,
                               ::fivetran_sdk::AlterTableResponse *response) {
  try {
    std::string db_name =
        find_property(request->configuration(), "motherduck_database");
    std::unique_ptr<Connection> con =
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

Status
DestinationSdkImpl::Truncate(::grpc::ServerContext *context,
                             const ::fivetran_sdk::TruncateRequest *request,
                             ::fivetran_sdk::TruncateResponse *response) {
  std::string db_name =
      find_property(request->configuration(), "motherduck_database");
  std::unique_ptr<Connection> con =
      get_connection(request->configuration(), db_name);

  truncate_table(*con, db_name, get_schema_name(request),
                 get_table_name(request));
  return ::grpc::Status(::grpc::StatusCode::OK, "");
}

Status
DestinationSdkImpl::WriteBatch(::grpc::ServerContext *context,
                               const ::fivetran_sdk::WriteBatchRequest *request,
                               ::fivetran_sdk::WriteBatchResponse *response) {

  try {
    auto schema_name = get_schema_name(request);

    std::string db_name =
        find_property(request->configuration(), "motherduck_database");
    std::unique_ptr<Connection> con =
        get_connection(request->configuration(), db_name);

    // Use local memory by default to prevent Arrow-based VIEW from traveling
    // up to the cloud
    con->Query("ATTACH ':memory:' as localmem");
    con->Query("USE localmem");

    const auto primary_keys = get_primary_keys(request->table().columns());
    const auto cols = get_duckdb_columns(request->table().columns());

    for (auto &filename : request->replace_files()) {
      auto decryption_key = get_encryption_key(filename, request->keys(),
                                               request->csv().encryption());
      auto convert_options = arrow::csv::ConvertOptions::Defaults();
      process_file(*con, filename, decryption_key, convert_options,
                   [&con, &db_name, &request, &primary_keys, &cols,
                    &schema_name](const std::string view_name) {
                     upsert(*con, db_name, schema_name, request->table().name(),
                            view_name, primary_keys, cols);
                   });
    }
    for (auto &filename : request->update_files()) {
      auto convert_options = arrow::csv::ConvertOptions::Defaults();
      // read all update-file CSV columns as text to accommodate
      // unmodified_string values
      std::vector<std::shared_ptr<arrow::DataType>> column_types(cols.size(),
                                                                 arrow::utf8());
      for (auto &col : cols) {
        convert_options.column_types.insert({col.name, arrow::utf8()});
      }

      auto decryption_key = get_encryption_key(filename, request->keys(),
                                               request->csv().encryption());
      process_file(*con, filename, decryption_key, convert_options,
                   [&con, &db_name, &request, &primary_keys, &cols,
                    &schema_name](const std::string view_name) {
                     update_values(*con, db_name, schema_name,
                                   request->table().name(), view_name,
                                   primary_keys, cols,
                                   request->csv().unmodified_string());
                   });
    }
    for (auto &filename : request->delete_files()) {
      auto convert_options = arrow::csv::ConvertOptions::Defaults();
      auto decryption_key = get_encryption_key(filename, request->keys(),
                                               request->csv().encryption());
      process_file(*con, filename, decryption_key, convert_options,
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

Status DestinationSdkImpl::Test(::grpc::ServerContext *context,
                                const ::fivetran_sdk::TestRequest *request,
                                ::fivetran_sdk::TestResponse *response) {

  try {
    std::string db_name =
        find_property(request->configuration(), "motherduck_database");
    std::unique_ptr<Connection> con =
        get_connection(request->configuration(), db_name);
    check_connection(*con);
  } catch (const std::exception &e) {
    response->set_failure(e.what());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
  }

  return ::grpc::Status(::grpc::StatusCode::OK, "");
}
