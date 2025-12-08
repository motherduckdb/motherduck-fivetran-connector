#pragma once

#include "duckdb.hpp"
#include <string>

struct column_def {
  std::string name;
  duckdb::LogicalTypeId type;
  bool primary_key;
  unsigned int width;
  unsigned int scale;
};

struct table_def {
  std::string db_name;
  std::string schema_name;
  std::string table_name;

  std::string to_escaped_string() const;
};