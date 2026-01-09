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
  enum class SinkType { NONE = 0, STDOUT = 1 << 0, DUCKDB = 1 << 1 };

  static Logger CreateNopLogger() { return Logger(SinkType::NONE); }

  static Logger CreateStdoutLogger() { return Logger(SinkType::STDOUT); }

  static Logger CreateMultiSinkLogger(duckdb::Connection *connection) {
    return Logger(connection);
  }

  void log(const std::string &level, const std::string &message) const;
  void info(const std::string &message) const;
  void warning(const std::string &message) const;
  void severe(const std::string &message) const;

private:
  // Used for NopLogger and StdoutLogger
  explicit Logger(SinkType sinks);

  // Logs to both stdout and DuckDB
  explicit Logger(duckdb::Connection *con_);

  SinkType enabled_sinks = SinkType::NONE;
  duckdb::Connection *con;
  std::string duckdb_id = "none";
  std::string connection_id = "none";
  mutable std::once_flag initialize_duckdb_logging_flag;

  static void log_to_stdout(const std::string &level,
                            const std::string &message);
  static void log_to_duckdb(duckdb::Connection &con, const std::string &level,
                            const std::string &message);
};
} // namespace mdlog
