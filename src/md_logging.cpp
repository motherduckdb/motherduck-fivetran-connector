#include <md_logging.hpp>

namespace mdlog {
std::string escape_char(const std::string &str, const char &c) {
  std::string result = str;
  int start_pos = 0;
  std::string escaped = std::string("\\") + c;
  while ((start_pos = result.find(c, start_pos)) != std::string::npos) {
    result.replace(start_pos, 1, escaped);
    start_pos += 2;
  }
  return result;
}

MdLog::MdLog(std::unique_ptr<logging_sink::LoggingSink::Stub>& client_): client(std::move(client_)) {}

void MdLog::log(const std::string level, const std::string message) {
  std::cout << "{\"level\":\"" << escape_char(level, '"') << "\","
            << "\"message\":\"" << escape_char(message, '"') << "\","
            << "\"message-origin\":\"sdk_destination\"}" << std::endl;
}

void MdLog::info(std::string message) { log("INFO", message); }

void MdLog::warning(std::string message) { log("WARNING", message); }

void MdLog::severe(std::string message) { log("SEVERE", message); }
} // namespace mdlog