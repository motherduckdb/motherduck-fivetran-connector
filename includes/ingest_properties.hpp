#pragma once

#include "schema_types.hpp"
#include <cstdint>
#include <string>
#include <vector>

enum class CompressionType { None = 0, ZSTD = 1 };

enum class UnmodifiedMarker { Disallowed = 0, Allowed = 1 };

struct IngestProperties {

  IngestProperties(const std::string &_filename,
                   const std::string &_decryption_key,
                   const std::vector<column_def> &_columns,
                   const std::string &_null_value,
                   const int &_csv_block_size_mb,
                   const UnmodifiedMarker _allow_unmodified_string,
                   const std::string &_temp_db_name)
      : filename(_filename), decryption_key(_decryption_key), columns(_columns),
        null_value(_null_value), csv_block_size_mb(_csv_block_size_mb),
        allow_unmodified_string(_allow_unmodified_string ==
                                UnmodifiedMarker::Allowed),
        temp_db_name(_temp_db_name) {}

  const std::string filename;
  const std::string decryption_key;
  /// Columns of the table that is being ingested into. Columns must be in the
  /// same order as they appear in the table.
  const std::vector<column_def> columns;
  /// String that represents NULL values in the CSV file.
  const std::string null_value;
  const std::uint32_t csv_block_size_mb;
  /// Indicates that the CSV file may contain "unmodified_string" values that
  /// should be treated as strings even if the target column is of a different
  /// type. In that case, the CSV file is read with all_varchar=true and type
  /// conversion is deferred to later stages (i.e., UPDATE).
  const bool allow_unmodified_string;
  /// Name of the temporary database used for history tables and CSV views.
  /// Callers of WriteBatch and WriteHistoryBatch are responsible for attaching
  /// and detaching this database.
  const std::string temp_db_name;
};