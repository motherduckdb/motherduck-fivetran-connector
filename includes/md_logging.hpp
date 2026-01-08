#pragma once

#include "duckdb.hpp"

#include <deque>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <utility>

namespace mdlog {

class Logger {
public:
  static Logger CreateStdoutLogger() {
    return Logger();
  }
  static Logger CreateMultiSinkLogger(duckdb::Connection *connection) {
    return Logger(connection);
  }

  void log(const std::string &level, const std::string &message) const;
  void info(const std::string &message) const;
  void warning(const std::string &message) const;
  void severe(const std::string &message) const;

private:
  // Only logs to stdout
  explicit Logger() { }

  // Logs to both stdout and DuckDB
  explicit Logger(duckdb::Connection *con_);

  bool enable_duckdb_logging;
  duckdb::Connection *con;
  std::string duckdb_id = "none";
  std::string connection_id = "none";
  std::once_flag enable_duckdb_logging_flag;

  static void log_to_stdout(const std::string &level, const std::string &message);
  static void log_to_duckdb(duckdb::Connection &con, const std::string &level, const std::string &message);
};
} // namespace mdlog
