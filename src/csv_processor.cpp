#include "csv_processor.hpp"

#include "decryption.hpp"
#include "duckdb.hpp"
#include "ingest_properties.hpp"
#include "md_logging.hpp"
#include "types.hpp"

#include <fstream>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>

namespace fs = std::filesystem;

namespace {
void add_projections(std::ostringstream &query,
                     const std::vector<column_def> &columns) {
  query << " SELECT";

  if (columns.empty()) {
    query << " *";
  } else {
    for (const auto &column : columns) {
      // DuckDB can handle trailing commas
      query << " " << duckdb::KeywordHelper::WriteQuoted(column.name, '"')
            << ",";
    }
  }
}

// TODO: Should take columns and allow_unmodified_string separately
void add_type_options(std::ostringstream &query,
                      const IngestProperties &props) {
  // We set all_varchar=true because we have to deal with
  // `unmodified_string`. Those are string values that represent an
  // unchanged value in an UPDATE or UPSERT, and they break type conversion
  // in the CSV reader. We do the conversion later during the UPDATE/UPSERT.
  // TODO: Can we push down type conversion to the CSV reader if there is no
  // `unmodified_string` specified? For all than update_files
  if (props.allow_unmodified_string) {
    query << ", all_varchar=true";
    return;
  }

  bool are_column_types_specified = false;
  for (const auto &column : props.columns) {
    if (column.type != duckdb::LogicalTypeId::INVALID) {
      are_column_types_specified = true;
      break;
    }
  }

  if (!are_column_types_specified) {
    // Try auto-detecting types if no columns are provided
    query << ", auto_detect=true";
    return;
  }

  // TODO: Can columns be empty?
  // We cannot assume the order of columns :-(
  // "Always read the CSV file header to determine the column order."
  // But I can use column_types to specify a mapping from column name to type:
  // column_types={'colB': 'VARCHAR', 'colA': 'INT'}
  query << ", column_types = {";
  for (const auto &column : props.columns) {
    if (column.type == duckdb::LogicalTypeId::INVALID) {
      continue;
    }

    // DuckDB can handle trailing comma
    query << duckdb::KeywordHelper::WriteQuoted(column.name, '\'') << ": '"
          << duckdb::EnumUtil::ToString(column.type) << "',";

    // TODO:
    // For DECIMAL types, include precision and scale if provided
    // if (column.type == duckdb::LogicalTypeId::DECIMAL &&
    //           column.width > 0) {
    //   query << "DECIMAL(" << column.width << "," << column.scale << ")";
    // }
  }
  query << "}";
  // We still need auto-detection for DuckDB to find out which columns exist,
  // even though we provide types.
  // TODO: I should write tests for DuckDB's CSV reader that validate some
  // assumptions I make.
  query << ", auto_detect=true";
}

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

// TODO: Mention that columns must be in order
std::string generate_read_csv_query(const std::string &filepath,
                                    const IngestProperties &props,
                                    const CompressionType compression) {
  std::ostringstream query;
  // TODO: Need to escape filename? Write test for this
  query << "FROM read_csv("
        << duckdb::KeywordHelper::WriteQuoted(filepath, '\'');
  query << ", delim=','";
  query << ", encoding='utf-8'";
  query << ", escape='\"'"; // TODO: Is this correct?
  query << ", header=true";
  query << ", new_line='\\n'";
  query << ", quote='\"'";
  // TODO: Specify DateFormat
  // Times have millisecond precision if I'm not mistaken
  // We here use nanoseconds
  // Example: 2024-01-09T04:10:19.156057706Z
  // TODO: We have to support both naive time and UTC time. Which timestamp
  // format to use? Auto-detect? query << ",
  // timestampformat='%Y-%m-%dT%H:%M:%S.%nZ'";
  if (!props.null_value.empty()) {
    query << ", nullstr='" << props.null_value << "'";
  }
  query << ", compression="
        << (compression == CompressionType::ZSTD ? "'zstd'" : "'none'");

  add_type_options(query, props);

  query << ")";

  // Select columns explicitly to enforce order
  add_projections(query, props.columns);

  return query.str();
}

void validate_file(const std::string &file_path) {
  const std::ifstream fs(file_path);
  if (fs.fail()) {
    // TODO: Throw with errno
    throw std::invalid_argument("File <" + file_path +
                                "> is missing or inaccessible");
  }
}

CompressionType determine_compression_type(const std::string &file_path) {
  std::ifstream fs(file_path, std::ios::binary);
  if (fs.fail()) {
    // TODO: Throw with errno
    throw std::invalid_argument("File <" + file_path + "> could not be open");
  }

  constexpr int MAGIC_SIZE = 4;
  uint8_t magic_bytes[MAGIC_SIZE];
  fs.read(reinterpret_cast<char *>(magic_bytes), sizeof(magic_bytes));

  if (!fs && !fs.eof()) {
    // TODO: Throw with errno
    throw std::invalid_argument("File <" + file_path +
                                "> failed to read magic bytes");
  }

  if (fs.gcount() < MAGIC_SIZE) {
    return CompressionType::None;
  }

  // Check for ZSTD magic number (0x28B52FFD in little-endian)
  const bool is_zstd_compressed =
      magic_bytes[0] == 0x28 && magic_bytes[1] == 0xB5 &&
      magic_bytes[2] == 0x2F && magic_bytes[3] == 0xFD;
  return is_zstd_compressed ? CompressionType::ZSTD : CompressionType::None;
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
  if (is_file_encrypted) {
    // TODO: Move to separate function
    std::vector<unsigned char> plaintext = decrypt_file(
        props.filename,
        reinterpret_cast<const unsigned char *>(props.decryption_key.c_str()));
    // Create a new temporary file
    // TODO: Use memory-backed file
    fs::path temp_dir = fs::temp_directory_path();
    fs::path temp_file = temp_dir / "temp_csv_file.csv";

    std::cout << "Temporary file: " << temp_file << "\n";

    // Create and write to the file
    std::ofstream ofs(temp_file,
                      std::ios::out | std::ios::binary | std::ios::trunc);
    if (!ofs) {
      // TODO: Throw with errno
      throw std::runtime_error(
          "Error opening temporary file for decrypted data");
    }
    // TODO: Check write success
    ofs.write(reinterpret_cast<const char *>(plaintext.data()),
              plaintext.size());
    ofs.close();
    ddb_file_path = temp_file;
    logger->info("Wrote temporary unencrypted file " + ddb_file_path);
  } else {
    logger->info("File is not encrypted");
  }

  auto compression = determine_compression_type(ddb_file_path);

  const auto con_id = con.context->GetConnectionId();
  const auto temp_db_name = "temp_mem_db_" + std::to_string(con_id);

  // Run DETACH just to be extra sure
  con.Query("DETACH DATABASE IF EXISTS " + temp_db_name);
  con.Query("ATTACH ':memory:' AS " + temp_db_name);

  std::string view_name = "\"" + temp_db_name + "\".\"main\".\"csv_view\"";

  // TODO: Move CREATE VIEW into generate function
  auto create_view_res =
      con.Query("CREATE VIEW " + view_name + " AS " +
                generate_read_csv_query(ddb_file_path, props, compression));
  if (create_view_res->HasError()) {
    create_view_res->ThrowError("Failed to create view for CSV file <" +
                                props.filename + ">");
  }
  logger->info("    view created for file " + props.filename);

  process_view(view_name);
  logger->info("    view processed for file " + props.filename);

  logger->info("    Detaching temp database " + temp_db_name + " for CSV view");
  con.Query("DETACH DATABASE IF EXISTS " + temp_db_name);
}
} // namespace csv_processor