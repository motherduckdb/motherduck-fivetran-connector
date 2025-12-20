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

struct table_def {
  std::string db_name;
  std::string schema_name;
  std::string table_name;

  [[nodiscard]] std::string to_escaped_string() const;
};