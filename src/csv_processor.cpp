#include "csv_processor.hpp"

#include "decryption.hpp"
#include "duckdb.hpp"
#include "ingest_properties.hpp"
#include "md_logging.hpp"
#include "memory_backed_file.hpp"
#include "schema_types.hpp"

#include <fstream>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unistd.h>

namespace fs = std::filesystem;

namespace {

void validate_file(const std::string &file_path) {
  const std::ifstream fs(file_path);
  if (fs.fail()) {
    throw std::system_error(errno, std::generic_category(),
                            "Failed to open file <" + file_path + ">");
  }
}

MemoryBackedFile
create_file_with_decrypted_content(const std::string &encrypted_file_path,
                                   const std::string &decryption_key) {
  // TODO: Let decrypt_file write into the memory-backed file directly to avoid
  // double buffering
  const std::vector<unsigned char> plaintext = decrypt_file(
      encrypted_file_path,
      reinterpret_cast<const unsigned char *>(decryption_key.c_str()));

  // Be defensive about casting errors from size_t (unsigned) to std::streamsize
  // (signed)
  constexpr auto max_file_size =
      static_cast<size_t>(std::numeric_limits<std::streamsize>::max());
  if (plaintext.size() > max_file_size) {
    throw std::runtime_error("Decrypted file size exceeds limit of " +
                             std::to_string(max_file_size) + " bytes");
  }

  auto temp_file = MemoryBackedFile::Create(plaintext.size());
  const auto decrypted_file_path = temp_file.path;

  std::ofstream ofs(decrypted_file_path, std::ios::binary);
  if (ofs.fail()) {
    throw std::system_error(
        errno, std::generic_category(),
        "Failed to open temporary output file for decrypted data with path <" +
            decrypted_file_path + ">");
  }

  ofs.write(reinterpret_cast<const char *>(plaintext.data()),
            static_cast<std::streamsize>(plaintext.size()));
  if (ofs.fail()) {
    throw std::system_error(
        errno, std::generic_category(),
        "Failed to write decrypted data to temporary file with path <" +
            decrypted_file_path + ">");
  }

  ofs.flush();
  if (ofs.fail()) {
    throw std::system_error(errno, std::generic_category(),
                            "Failed to flush ofstream for path <" +
                                decrypted_file_path + ">");
  }

  // Reset cursor to the beginning in case the reader expects this. See
  // memory_backed_file.hpp.
  ofs.seekp(0);
  if (ofs.fail()) {
    throw std::system_error(
        errno, std::generic_category(),
        "Failed to seek to beginning of ofstream for path <" +
            decrypted_file_path + ">");
  }

  // Close explicitly to check for errors
  ofs.close();
  if (ofs.fail()) {
    throw std::system_error(errno, std::generic_category(),
                            "Failed to close ofstream for path <" +
                                decrypted_file_path + ">");
  }

  return temp_file;
}

void reset_file_cursor(int file_descriptor) {
  // For memory-backed files accessed via /dev/fd/<n>, the cursor is shared
  // across all file descriptors on macOS. Reset it to the beginning so that
  // subsequent reads start from the beginning.
  if (lseek(file_descriptor, 0, SEEK_SET) == -1) {
    throw std::system_error(errno, std::generic_category(),
                            "Failed to reset file cursor");
  }
}

CompressionType determine_compression_type(const std::string &file_path) {
  std::ifstream ifs(file_path, std::ios::binary);
  if (ifs.fail()) {
    throw std::system_error(errno, std::generic_category(),
                            "Failed to open file <" + file_path + ">");
  }

  constexpr int MAGIC_SIZE = 4;
  uint8_t magic_bytes[MAGIC_SIZE];
  ifs.read(reinterpret_cast<char *>(magic_bytes), sizeof(magic_bytes));

  if (ifs.fail() && !ifs.eof()) {
    throw std::system_error(
        errno, std::generic_category(),
        "Failed trying to read zstd magic bytes from file <" + file_path + ">");
  }

  // File has fewer than 4 bytes, hence cannot be zstd-compressed
  if (ifs.gcount() < MAGIC_SIZE) {
    return CompressionType::None;
  }

  // Check for ZSTD magic number (0x28B52FFD in little-endian)
  const bool is_zstd_compressed =
      magic_bytes[0] == 0x28 && magic_bytes[1] == 0xB5 &&
      magic_bytes[2] == 0x2F && magic_bytes[3] == 0xFD;
  return is_zstd_compressed ? CompressionType::ZSTD : CompressionType::None;
}

/// Adds a SELECT clause with the specified columns to the query
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

/// Adds CSV reader options related to column types to the query (all_varchar or
/// column_types)
void add_type_options(std::ostringstream &query,
                      const std::vector<column_def> &columns,
                      const bool allow_unmodified_string) {
  // We set all_varchar=true if we have to deal with `unmodified_string`. Those
  // are string values that represent an unchanged value in an UPDATE or UPSERT,
  // and they break type conversion in the CSV reader. DuckDB does an implicit
  // conversion later during the UPDATE/UPSERT. If `unmodified_string` is not
  // set (for UPSERT and DELETE), we push down the type conversion to the CSV
  // reader.
  if (allow_unmodified_string) {
    query << ", all_varchar=true";
    return;
  }

  bool has_valid_column_types = false;
  for (const auto &column : columns) {
    if (column.type != duckdb::LogicalTypeId::INVALID) {
      has_valid_column_types = true;
      break;
    }
  }

  if (!has_valid_column_types) {
    // No valid column types. We need to back out because column_types must not
    // be an empty struct.
    return;
  }

  // We cannot assume the order of columns. From the Fivetran Partner SDK docs:
  // "Always read the CSV file header to determine the column order."
  // Therefore, we do not set the `columns` parameter, but `column_types` which
  // is a mapping from a column name to a type:
  // column_types={'colB':'VARCHAR','colA':'INTEGER'}.
  // DuckDB detects the order of columns by reading the header row.
  // If no columns are specified, DuckDB will auto-detect all column types.
  query << ", column_types={";
  for (const auto &column : columns) {
    // Even if we do not specify the type for this column, DuckDB will figure it
    // out itself because of auto_detect=true
    if (column.type == duckdb::LogicalTypeId::INVALID) {
      continue;
    }

    query << duckdb::KeywordHelper::WriteQuoted(column.name, '\'') << ":";

    query << "'" << duckdb::EnumUtil::ToString(column.type);
    if (column.type == duckdb::LogicalTypeId::DECIMAL && column.width > 0) {
      query << "(" << std::to_string(column.width) << ","
            << std::to_string(column.scale) + ")";
    }
    // DuckDB can handle trailing comma
    query << "',";
  }
  query << "}";
}

/// Generates a DuckDB SQL query string to read a CSV file with the specified
/// properties
std::string generate_read_csv_query(const std::string &filepath,
                                    const IngestProperties &props,
                                    const CompressionType compression) {
  std::ostringstream query;
  query << "FROM read_csv("
        << duckdb::KeywordHelper::WriteQuoted(filepath, '\'');
  // We set auto_detect=true so that DuckDB can detect the dialect options that
  // we do not set explicitly. It further helps with detecting column types if
  // there happen to be columns whose type we did not set explicitly. This is
  // not expected to happen, but is more robust this way.
  query << ", auto_detect=true";
  query << ", delim=','";
  query << ", encoding='utf-8'";
  // Escaped string in CSV looks like this: "A ""quoted"" word"
  query << ", escape='\"'";
  query << ", header=true";
  query << ", new_line='\\n'";
  query << ", quote='\"'";
  // We do not specify timestampformat, see below.
  // Date format: 2025-12-31
  query << ", dateformat='%Y-%m-%d'";
  if (!props.null_value.empty()) {
    query << ", nullstr="
          << duckdb::KeywordHelper::WriteQuoted(props.null_value, '\'');
    query << ", allow_quoted_nulls=true";
  }
  // The default max_line_size is 2MB. The default buffer size is
  // 16*max_line_size=32MB. We only set this option to a higher value if
  // requested.
  constexpr std::uint32_t default_csv_block_size_mb = 32;
  if (props.csv_block_size_mb > default_csv_block_size_mb) {
    const auto max_line_size = props.csv_block_size_mb * 1000 * 1000 / 16;
    query << ", max_line_size=" << std::to_string(max_line_size);
    query << ", buffer_size=" << std::to_string(max_line_size * 16);
  }
  query << ", compression="
        << (compression == CompressionType::ZSTD ? "'zstd'" : "'none'");

  // We do not specify timestampformat because CSV files can contain two
  // different formats:
  // - %Y-%m-%dT%H:%M:%S.%nZ (UTC time) and
  // - %Y-%m-%dT%H:%M:%S.%n (naive time)
  // We cannot specify both formats at the same time, hence DuckDB needs to
  // auto-detect them. Times have millisecond precision if I'm not mistaken We
  // here use nanoseconds Example: 2024-01-09T04:10:19.156057706Z

  add_type_options(query, props.columns, props.allow_unmodified_string);

  query << ")";

  // Select columns explicitly to enforce order
  add_projections(query, props.columns);

  return query.str();
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
  std::string decrypted_file_path;
  // Only used if file is encrypted to ensure MemoryBackedFile lives long enough
  std::optional<MemoryBackedFile> temp_file;
  if (is_file_encrypted) {
    temp_file = create_file_with_decrypted_content(props.filename,
                                                   props.decryption_key);
    decrypted_file_path = temp_file.value().path;
    logger->info("    wrote temporary unencrypted file " + decrypted_file_path);
  } else {
    decrypted_file_path = props.filename;
    logger->info("    file is not encrypted");
  }

  if (temp_file.has_value()) {
    reset_file_cursor(temp_file.value().fd);
  }

  auto compression = determine_compression_type(decrypted_file_path);

  // The last function call read four bytes. Reset to the beginning again.
  if (temp_file.has_value()) {
    reset_file_cursor(temp_file.value().fd);
  }

  const auto con_id = con.context->GetConnectionId();
  const auto temp_db_name = "temp_mem_db_" + std::to_string(con_id);

  // Run DETACH just to be extra sure
  con.Query("DETACH DATABASE IF EXISTS " + temp_db_name);
  const auto attach_res = con.Query("ATTACH ':memory:' AS " + temp_db_name);
  if (attach_res->HasError()) {
    attach_res->ThrowError("Failed to attach in-memory database \"" +
                           temp_db_name + "\": ");
  }

  std::string view_name = "\"" + temp_db_name + R"("."main"."csv_view")";

  const auto final_query =
      "CREATE VIEW " + view_name + " AS " +
      generate_read_csv_query(decrypted_file_path, props, compression);
  logger->info("    creating view: " + final_query);
  auto create_view_res = con.Query(final_query);
  if (create_view_res->HasError()) {
    create_view_res->ThrowError("Failed to create view for CSV file <" +
                                props.filename + ">: ");
  }
  logger->info("    view created for file " + props.filename);

  // `read_csv` opened and read the file for binding. Reset the file cursor
  // again for execution.
  if (temp_file.has_value()) {
    reset_file_cursor(temp_file.value().fd);
  }

  process_view(view_name);
  logger->info("    view processed for file " + props.filename);

  logger->info("    detaching temp database " + temp_db_name + " for CSV view");

  // Ignore errors during DETACH
  con.Query("DETACH DATABASE IF EXISTS " + temp_db_name);
}
} // namespace csv_processor