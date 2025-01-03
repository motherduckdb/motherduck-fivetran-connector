#pragma once

#include <iostream>
#include <map>

namespace mdlog {

class MdLog {
public:
  void log(const std::string &level, const std::string &message);

  void info(const std::string &message);

  void warning(const std::string &message);

  void severe(const std::string &message);

  void set_duckdb_id(const std::string &duckdb_id_);

private:
  std::string duckdb_id = "none";
};

std::string escape_char(const std::string &str, const char &c);

} // namespace mdlog
