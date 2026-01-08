#include "md_logging.hpp"

#include <iostream>
#include <mutex>
#include <string>

namespace mdlog {

namespace {
void initialize_duckdb_logging(duckdb::Connection &con) {
  con.Query("CALL enable_logging()");
  con.Query("SET logging_storage='motherduck_log_storage'");
  con.Query("SET logging_level='info'");
  con.Query("SET motherduck_log_level='info'");
}

std::string escape_char(const std::string &str, const char &c) {
  std::string result = str;
  int start_pos = 0;
  const std::string escaped = std::string("\\") + c;
  while ((start_pos = result.find(c, start_pos)) != std::string::npos) {
    result.replace(start_pos, 1, escaped);
    start_pos += 2;
  }
  return result;
}

std::string escape_single_quote(const std::string &str) {
  std::string result = str;
  size_t start_pos = 0;
  while ((start_pos = result.find('\'', start_pos)) != std::string::npos) {
    result.replace(start_pos, 1, "''");
    start_pos += 2;
  }
  return result;
}
} // namespace

Logger::Logger(duckdb::Connection *con_) : enable_duckdb_logging(true), con(con_) {
  const auto client_ids_res =
      con->Query("SELECT md_current_client_duckdb_id(), "
                 "md_current_client_connection_id()");
  if (client_ids_res->HasError()) {
    log_to_stdout("WARNING", "Could not retrieve the current DuckDB and connection ID: " + client_ids_res->GetError());
  } else {
    duckdb_id = client_ids_res->GetValue(0, 0).ToString();
    connection_id = client_ids_res->GetValue(1, 0).ToString();
  }
}

void Logger::log_to_stdout(const std::string &level, const std::string &message) {
  std::cout << message << std::endl;
}

void Logger::log_to_duckdb(duckdb::Connection &con, const std::string &level, const std::string &message) {
  const std::string query = "SELECT write_log('" + escape_single_quote(message) +
                            "', log_type:='Fivetran', level:='" + escape_single_quote(level) + "')";
  // Ignore errors from the query
  con.Query(query);
}

  void Logger::log(const std::string &level, const std::string &message) const {
  std::stringstream full_message_stream;
  full_message_stream << "{\"level\":\"" << escape_char(level, '"') << "\","
            << "\"message\":\"" << escape_char(message, '"') << ", duckdb_id=<"
            << duckdb_id << ">, connection_id=<" << connection_id << ">\","
            << "\"message-origin\":\"sdk_destination\"}";
  const auto full_message = full_message_stream.str();

  log_to_stdout(level, full_message);

  if (enable_duckdb_logging) {
    std::call_once(enable_duckdb_logging_flag, [&]() {
      initialize_duckdb_logging(*con);
    });
    log_to_duckdb(*con, level, full_message);
  }
}

void Logger::info(const std::string &message) const { log("INFO", message); }

void Logger::warning(const std::string &message) const {
  log("WARNING", message);
}

void Logger::severe(const std::string &message) const { log("SEVERE", message); }

} // namespace mdlog