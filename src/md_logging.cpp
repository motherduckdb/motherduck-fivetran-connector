#include "md_logging.hpp"

#include <iostream>
#include <string>

namespace mdlog {

namespace {
std::string escape_char(const std::string &str, const char &c) {
  std::string result = str;
  size_t start_pos = 0;
  const std::string escaped = std::string("\\") + c;
  while ((start_pos = result.find(c, start_pos)) != std::string::npos) {
    result.replace(start_pos, 1, escaped);
    start_pos += 2;
  }
  return result;
}
} // namespace

void MdLog::log(const std::string &level, const std::string &message) const {
  std::cout << "{\"level\":\"" << escape_char(level, '"') << "\","
            << "\"message\":\"" << escape_char(message, '"') << ", duckdb_id=<"
            << duckdb_id << ">, connection_id=<" << connection_id << ">\","
            << "\"message-origin\":\"sdk_destination\"}" << std::endl;
}

void MdLog::info(const std::string &message) const { log("INFO", message); }

void MdLog::warning(const std::string &message) const {
  log("WARNING", message);
}

void MdLog::severe(const std::string &message) const { log("SEVERE", message); }

void MdLog::set_duckdb_id(const std::string &duckdb_id_) {
  duckdb_id = duckdb_id_;
}

void MdLog::set_connection_id(const std::string &connection_id_) {
  connection_id = connection_id_;
}

} // namespace mdlog