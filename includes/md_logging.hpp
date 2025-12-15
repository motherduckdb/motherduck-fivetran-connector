#pragma once

#include "duckdb.hpp"

#include <deque>
#include <iostream>
#include <map>
#include <string>
#include <utility>

namespace mdlog {

class MdLog {
public:
  void log(const std::string &level, const std::string &message);

  void info(const std::string &message);
  void warning(const std::string &message);
  void severe(const std::string &message);

  void set_duckdb_id(const std::string &duckdb_id_);
  void set_connection_id(const std::string &connection_id_);
  void set_connection(duckdb::Connection *connection_);

private:
  static constexpr size_t MAX_BUFFERED_MESSAGES = 64;

  void log_to_stdout(const std::string &level, const std::string &message);
  void log_to_duckdb(const std::string &level, const std::string &message);
  void flush_buffer();

  std::string duckdb_id = "none";
  std::string connection_id = "none";
  duckdb::Connection *connection = nullptr;
  std::deque<std::pair<std::string, std::string>> buffered_messages;
};

} // namespace mdlog
