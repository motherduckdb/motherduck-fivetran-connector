#pragma once

#include "schema_types.hpp"
#include <string>
#include <vector>

enum class CompressionType { None = 0, ZSTD = 1 };

struct IngestProperties {
  std::string filename;
  /// Binary key used to decrypt the CSV file. Empty if the file is not
  /// encrypted.
  std::string decryption_key;
  /// Columns of the table that is being ingested into. Columns must be in the
  /// same order as they appear in the table.
  std::vector<column_def> columns;
  /// String that represents NULL values in the CSV file.
  std::string null_value;
  /// Indicates that the CSV file may contain "unmodified_string" values that
  /// should be treated as strings even if the target column is of a different
  /// type. In that case, the CSV file is read with all_varchar=true and type
  /// conversion is deferred to later stages (i.e., UPDATE).
  bool allow_unmodified_string = false;
};
