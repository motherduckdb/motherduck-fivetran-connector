#include "csv_processor.hpp"

#include "csv_arrow_ingest.hpp"
#include "decryption.hpp"
#include "duckdb.hpp"
#include "ingest_properties.hpp"
#include "md_logging.hpp"

#include <arrow/c/bridge.h>
#include <fstream>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>

namespace fs = std::filesystem;

namespace {
  void add_csv_reader_options(std::stringstream &ss,
                            std::vector<column_def> &columns,
                            IngestProperties &props) {
  ss << "columns = {";
  for (const auto &column : columns) {
    ss << duckdb::KeywordHelper::WriteQuoted(column.name, '\'');
    // TODO: Should check that column.type is something we can handle?
    ss << duckdb::KeywordHelper::WriteQuoted(
        duckdb::EnumUtil::ToString(column.type), '\'');
  }
  ss << "}";

  // allow_quoted_nulls = false: We want to interpret "NULL" as a literal string
  // escape???
  // force_not_null: Define columns where there cannot be a null-string to skip
  // comparison there ignore_errors? How do we want to handle errors? Can we use
  // the reject_table? It should work because the read is local. max_line_size?
  // Is the default of 2000000 bytes fine? new_line should probably be set to
  // '\n' columns or column_types

  // Configure preserve_insertion_order
  // Other configuration options: TimeZone?
  // autoinstall_known_extensions, autoload_known_extensions should be false
  // Disable HTTP file system
  // Check which DuckDB extensions are installed. Should be none by default.
  // Can we get rid of old_implicit_casting?
  // preserve_identifier_case to make identifiers case-insensitive. Any benefit?
  // enable_progress_bar = false
  // At some point, I can enable profiling

  // unmodified_string indicates that the value in this column of the tuple was
  // not changed. Can we do something smart with this while reading already?

  // Test reading CSV with escape character in quoted string (should be ""?)

  // I need a test with unmodified_string. How do I deal with the fact that this
  // string does not match the column type?

  // When do I get an unmodified_string? Only for updates/upserts? Then I can
  // have special handling for this and otherwise give the CSV reader its types?
}

std::string generate_read_csv_query(const std::string &filepath,
                                    const std::vector<column_def> &columns,
                                    const IngestProperties &props,
                                    std::string compression) {
  // Defaults:
  // - Encoding: UTF-8

  std::ostringstream query;
  // TODO: Need to escape filename? Write test for this
  query << "SELECT * FROM read_csv('" << filepath << "'",
      // We set all_varchar=true because we have to deal with
      // `unmodified_string`. Those are string values that represent an
      // unchanged value in an UPDATE or UPSERT, and they break type conversion
      // in the CSV reader. We do the conversion later during the UPDATE/UPSERT.
      // TODO: Can we push down type conversion to the CSV reader if there is no
      // `unmodified_string` specified?
      query << ", all_varchar=true";
  query << ", delim=','";
  query << ", escape='\"'"; // TODO: Is this correct?
  query << ", encoding='utf-8'";
  query << ", header=true";
  query << ", new_line='\\n'";
  query << ", quote='\"'";
  // Times have millisecond precision if I'm not mistaken
  // We here use nanoseconds
  // Example: 2024-01-09T04:10:19.156057706Z
  query << ", timestampformat='%Y-%m-%dT%H:%M:%S.%nZ'";
  if (!props.null_value.empty()) {
    query << ", nullstr='" << props.null_value << "'";
  }
  // query << ", compression=" << (props.compression == CompressionType::ZSTD ?
  // "'zstd'" : "'uncompressed'");
  query << ", compression='" + compression + "'";

  // TODO: Can columns be empty?
  if (!columns.empty()) {
    query << ", columns = {";
    bool first = true;
    for (const auto &column : columns) {
      if (!first) {
        query << ", ";
      }
      first = false;

      // Quote the column name
      // TODO: Potentially escape column name
      query << "'" << column.name << "': 'VARCHAR'";

      // For DECIMAL types, include precision and scale if provided
      // if (column.type == duckdb::LogicalTypeId::DECIMAL &&
      //           column.width > 0) {
      //   query << "DECIMAL(" << column.width << "," << column.scale << ")";
      // }
    }
    query << "}";
    query << ", auto_detect=false";
  } else {
    query << ", auto_detect=true";
  }

  query << ")";
  return query.str();
}
} // namespace experimental

namespace {
void validate_file(const std::string &file_path) {
  std::ifstream fs(file_path.c_str());
  if (fs.good()) {
    fs.close();
    return;
  }
  throw std::invalid_argument("File <" + file_path +
                              "> is missing or inaccessible");
}
} // namespace

namespace csv_processor {
void ProcessFile(
    duckdb::Connection &con, const IngestProperties &props,
    std::shared_ptr<mdlog::MdLog> &logger,
    const std::function<void(const std::string &view_name)> &process_view) {

  validate_file(props.filename);
  logger->info("    validated file " + props.filename);

  const auto is_file_encrypted = !props.decryption_key.empty();
  std::string ddb_file_path = props.filename;
  std::string compression = "UNCOMPRESSED";
  if (is_file_encrypted) {
    std::vector<unsigned char> plaintext = decrypt_file(
        props.filename,
        reinterpret_cast<const unsigned char *>(props.decryption_key.c_str()));
    // Create a new temporary file
    fs::path temp_dir = fs::temp_directory_path();
    fs::path temp_file = temp_dir / "temp_csv_file.csv";

    std::cout << "Temporary file: " << temp_file << "\n";

    // Create and write to the file
    std::ofstream ofs(temp_file,
                      std::ios::out | std::ios::binary | std::ios::trunc);
    if (!ofs) {
      // TODO: Throw with errno
      throw std::runtime_error("Error opening temporary file for decrypted data");
    }
    // TODO: Check write success
    ofs.write(reinterpret_cast<const char *>(plaintext.data()),
              plaintext.size());
    ofs.close();
    ddb_file_path = temp_file;
    compression = "ZSTD";
    // TODO: Should check zstd magic bytes
    logger->info("Wrote temporary file " + ddb_file_path);
  } else {
    logger->info("File is not encrypted");
  }

  const auto con_id = con.context->GetConnectionId();
  const auto temp_db_name = "temp_mem_db_" + std::to_string(con_id);

  // Run DETACH just to be extra sure
  con.Query("DETACH DATABASE IF EXISTS " + temp_db_name);
  con.Query("ATTACH ':memory:' AS " + temp_db_name);
  // Use local memory by default to prevent Arrow-based VIEW from traveling
  // up to the cloud
  con.Query("USE " + temp_db_name);

  // TODO: Move CREATE VIEW into generate function
  auto create_view_res =
      con.Query("CREATE VIEW " + temp_db_name + ".main.arrow_view AS " +
                generate_read_csv_query(ddb_file_path, {}, props, compression));
  if (create_view_res->HasError()) {
    create_view_res->ThrowError("Failed to create view for CSV file <" + props.filename + ">");
  }
  logger->info("    view created for file " + props.filename);

  process_view("\"" + temp_db_name + "\".\"arrow_view\"");
  logger->info("    view processed for file " + props.filename);

  logger->info("    Detaching temp database " + temp_db_name + " for CSV view");
  con.Query("DETACH DATABASE IF EXISTS " + temp_db_name);
  arrow_array_stream.release(&arrow_array_stream);
}
} // namespace csv_processor