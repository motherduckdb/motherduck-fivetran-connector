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

MdLog::MdLog(const std::string &token_, std::shared_ptr<logging_sink::LoggingSink::Stub>& client_):
  token(token_), client(std::move(client_)) {}

void MdLog::log(const std::string &level, const std::string &message) {
  std::ostringstream json_stream;
  json_stream << "{\"level\":\"" << escape_char(level, '"') << "\","
            << "\"message\":\"" << escape_char(message, '"') << "\","
            << "\"message-origin\":\"sdk_destination\"}";
  auto json_log_entry = json_stream.str();
  std::cout << json_log_entry << std::endl;
  auto context = std::make_shared<grpc::ClientContext>();
  context->AddMetadata("x-md-token",token);

  // just one log event, for testing
  logging_sink::LogEventBatchRequest request;
  logging_sink::LogEventBatchResponse response;
  request.add_log_events()->set_json_line(json_log_entry);

  const auto result = client->LogEventBatch(context.get(), request, &response);
  std::cout << "****** result of grpc log batch call: "
      << result.ok() << " (error code = " << to_string(result.error_code()) + ")"
      << " (error message = " << result.error_message() + ")";
}

void MdLog::info(std::string message) { log( "INFO", message); }

void MdLog::warning(std::string message) { log( "WARNING", message); }

void MdLog::severe(std::string message) { log("SEVERE", message); }
} // namespace mdlog