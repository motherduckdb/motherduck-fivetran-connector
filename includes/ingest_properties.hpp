#pragma once

#include <string>
#include <vector>

struct IngestProperties {

  IngestProperties(const std::string &_filename,
                   const std::string &_decryption_key,
                   const std::vector<std::string> &_utf8_columns,
                   const std::string &_null_value,
                   const int &_csv_block_size_mb)
      : filename(_filename), decryption_key(_decryption_key),
        utf8_columns(_utf8_columns), null_value(_null_value),
        csv_block_size_mb(_csv_block_size_mb) {}

  const std::string filename;
  const std::string decryption_key;
  const std::vector<std::string> utf8_columns;
  const std::string null_value;
  const int csv_block_size_mb;
};