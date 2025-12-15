#include "md_logging.hpp"

#include <iostream>
#include <string>

namespace mdlog {

namespace {
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

void MdLog::log_to_stdout(const std::string &level, const std::string &message) {
  std::cout << "{\"level\":\"" << escape_char(level, '"') << "\","
            << "\"message\":\"" << escape_char(message, '"') << ", duckdb_id=<"
            << duckdb_id << ">, connection_id=<" << connection_id << ">\","
            << "\"message-origin\":\"sdk_destination\"}" << std::endl;
}

void MdLog::log_to_duckdb(const std::string &level, const std::string &message) {
  if (connection == nullptr) {
    return;
  }
  const std::string full_message = message + ", duckdb_id=<" + duckdb_id +
                                   ">, connection_id=<" + connection_id + ">";
  const std::string query = "SELECT write_log('" + escape_single_quote(full_message) +
                            "', log_type:='Fivetran', level:='" + escape_single_quote(level) + "')";
  // Ignore errors from the query
  connection->Query(query);
}

void MdLog::flush_buffer() {
  for (const auto &entry : buffered_messages) {
    log_to_duckdb(entry.first, entry.second);
  }
  buffered_messages.clear();
}

void MdLog::log(const std::string &level, const std::string &message) {
  log_to_stdout(level, message);

  if (connection != nullptr) {
    log_to_duckdb(level, message);
  } else {
    buffered_messages.emplace_back(level, message);
    if (buffered_messages.size() > MAX_BUFFERED_MESSAGES) {
      buffered_messages.pop_front();
    }
  }
}

void MdLog::info(const std::string &message) { log("INFO", message); }

void MdLog::warning(const std::string &message) {
  log("WARNING", message);
}

void MdLog::severe(const std::string &message) { log("SEVERE", message); }

void MdLog::set_duckdb_id(const std::string &duckdb_id_) {
  duckdb_id = duckdb_id_;
}

void MdLog::set_connection_id(const std::string &connection_id_) {
  connection_id = connection_id_;
}

void MdLog::set_connection(duckdb::Connection *connection_) {
  connection = connection_;
  flush_buffer();
}

} // namespace mdlog