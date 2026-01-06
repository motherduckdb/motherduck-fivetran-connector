#pragma once

#include "duckdb.hpp"
#include <cstdint>
#include <string>

struct column_def {
  std::string name;
  duckdb::LogicalTypeId type;
  bool primary_key;
  std::uint32_t width;
  std::uint32_t scale;
};

inline std::ostream &operator<<(std::ostream &os, const column_def &col) {
  os << duckdb::EnumUtil::ToChars(col.type);
  if (col.type == duckdb::LogicalTypeId::DECIMAL) {
    os << " (" << col.width << "," << col.scale << ")";
  }
  return os;
}

struct table_def {
  std::string db_name;
  std::string schema_name;
  std::string table_name;

  [[nodiscard]] std::string to_escaped_string() const;
};