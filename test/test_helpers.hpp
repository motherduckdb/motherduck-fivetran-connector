#pragma once

#include "ingest_properties.hpp"
#include "schema_types.hpp"

#include <string>
#include <vector>

namespace test_helpers {

// Helper to create column_def with default values for optional fields
inline column_def col(const std::string &name, duckdb::LogicalTypeId type,
                      bool primary_key = false, std::uint8_t width = 0,
                      std::uint8_t scale = 0) {
  return column_def{
      .name = name,
      .type = type,
      .primary_key = primary_key,
      .width = width,
      .scale = scale,
  };
}

// Helper to create a primary key column
inline column_def pk(const std::string &name, duckdb::LogicalTypeId type) {
  return col(name, type, true);
}

// Helper to create a decimal column
inline column_def decimal_col(const std::string &name, std::uint8_t width,
                              std::uint8_t scale, bool primary_key = false) {
  return col(name, duckdb::LogicalTypeId::DECIMAL, primary_key, width, scale);
}

// Helper to create IngestProperties with default values
inline IngestProperties make_props(const std::string &filename,
                                   const std::vector<column_def> &columns,
                                   const std::string &decryption_key = "",
                                   const std::string &null_value = "",
                                   bool allow_unmodified_string = false) {
  return IngestProperties{
      .filename = filename,
      .decryption_key = decryption_key,
      .columns = columns,
      .null_value = null_value,
      .allow_unmodified_string = allow_unmodified_string,
  };
}

} // namespace test_helpers
