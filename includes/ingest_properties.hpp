#pragma once

#include "schema_types.hpp"
#include <string>
#include <vector>

enum class CompressionType { None = 0, ZSTD = 1 };

struct IngestProperties {

  IngestProperties(const std::string &_filename,
                   const std::string &_decryption_key,
                   const std::vector<column_def> &_columns,
                   const std::string &_null_value,
                   const int &_csv_block_size_mb,
                   const bool _allow_unmodified_string)
      : filename(_filename), decryption_key(_decryption_key),
        compression(CompressionType::None), columns(_columns),
        null_value(_null_value), csv_block_size_mb(_csv_block_size_mb), allow_unmodified_string(_allow_unmodified_string) {}

  const std::string filename;
  const std::string decryption_key;
  const CompressionType compression;
  const std::vector<column_def> columns;
  const std::string null_value;
  const int csv_block_size_mb;
  const bool allow_unmodified_string;
};