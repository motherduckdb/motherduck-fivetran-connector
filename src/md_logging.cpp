#include "md_logging.hpp"
#include "duckdb.hpp"

#include <iostream>
#include <mutex>
#include <string>

namespace mdlog {

namespace {

Logger::SinkType operator|(Logger::SinkType lhs, Logger::SinkType rhs) {
  return static_cast<Logger::SinkType>(static_cast<int>(lhs) |
                                       static_cast<int>(rhs));
}

bool HasFlag(const Logger::SinkType value, const Logger::SinkType flag) {
  return static_cast<Logger::SinkType>(static_cast<int>(value) &
                                       static_cast<int>(flag)) !=
         Logger::SinkType::NONE;
}

void initialize_duckdb_logging(duckdb::Connection &con) {
  con.Query("CALL enable_logging()");
  con.Query("SET logging_storage='motherduck_log_storage'");
  con.Query("SET logging_level='info'");
  con.Query("SET motherduck_log_level='info'");
}

std::string create_json_log_message(const std::string &level,
                                    const std::string &message,
                                    const std::string &duckdb_id,
                                    const std::string &connection_id) {
  std::ostringstream full_message_stream;
  full_message_stream << "{\"level\":\""
                      << duckdb::KeywordHelper::EscapeQuotes(level, '"')
                      << "\","
                      << "\"message\":\""
                      << duckdb::KeywordHelper::EscapeQuotes(message, '"')
                      << ", duckdb_id=<" << duckdb_id << ">, connection_id=<"
                      << connection_id << ">\","
                      << "\"message-origin\":\"sdk_destination\"}";
  return full_message_stream.str();
}

} // namespace

Logger::Logger(const SinkType sinks) : enabled_sinks(sinks) {
  // The other constructor should be used for DuckDB logging
  assert(!HasFlag(enabled_sinks, SinkType::DUCKDB));
}

Logger::Logger(duckdb::Connection *con_)
    : enabled_sinks(SinkType::STDOUT | SinkType::DUCKDB), con(con_) {
  assert(con != nullptr);
  const auto client_ids_res =
      con->Query("SELECT md_current_client_duckdb_id(), "
                 "md_current_client_connection_id()");
  if (client_ids_res->HasError()) {
    const auto error_msg = create_json_log_message(
        "WARNING",
        "Could not retrieve the current DuckDB and connection ID: " +
            client_ids_res->GetError(),
        "none", "none");
    log_to_stdout(error_msg);
  } else {
    duckdb_id = client_ids_res->GetValue(0, 0).ToString();
    connection_id = client_ids_res->GetValue(1, 0).ToString();
  }
}

void Logger::log_to_stdout(const std::string &message) {
  std::cout << message << std::endl;
}

void Logger::log_to_duckdb(duckdb::Connection &con, const std::string &level,
                           const std::string &message) {
  const std::string query = "SELECT write_log(" +
                            duckdb::KeywordHelper::WriteQuoted(message, '\'') +
                            ", log_type:='Fivetran', level:=" +
                            duckdb::KeywordHelper::WriteQuoted(level) + ")";
  // Ignore errors from the query
  con.Query(query);
}

void Logger::log(const std::string &level, const std::string &message) const {
  const auto json_message =
      create_json_log_message(level, message, duckdb_id, connection_id);
  if (HasFlag(enabled_sinks, SinkType::STDOUT)) {
    log_to_stdout(json_message);
  }

  if (HasFlag(enabled_sinks, SinkType::DUCKDB)) {
    std::call_once(
        initialize_duckdb_logging_flag,
        [](duckdb::Connection &con) { initialize_duckdb_logging(con); }, *con);
    log_to_duckdb(*con, level, json_message);
  }
}

void Logger::info(const std::string &message) const { log("INFO", message); }

void Logger::warning(const std::string &message) const {
  log("WARNING", message);
}

void Logger::severe(const std::string &message) const {
  log("SEVERE", message);
}

} // namespace mdlog