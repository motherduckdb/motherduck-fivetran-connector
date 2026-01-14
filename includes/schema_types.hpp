#pragma once

#include "duckdb.hpp"
#include <cstdint>
#include <string>

struct column_def {
  std::string name;
  duckdb::LogicalTypeId type;
  std::string column_default;
  bool primary_key;
  std::uint32_t width;
  std::uint32_t scale;
};

inline std::string format_type(const column_def &col) {
  if (col.type == duckdb::LogicalTypeId::DECIMAL) {
    return duckdb::EnumUtil::ToString(col.type) + " (" +
           std::to_string(col.width) + "," + std::to_string(col.scale) + ")";
  }

  return duckdb::EnumUtil::ToChars(col.type);
}

struct table_def {
  std::string db_name;
  std::string schema_name;
  std::string table_name;

  [[nodiscard]] std::string to_escaped_string() const;
};