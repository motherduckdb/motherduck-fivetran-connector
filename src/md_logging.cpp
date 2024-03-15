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
  context->AddMetadata("x-md-duckdb-version","v0.9.2");
  context->AddMetadata("x-md-extension-version","v1.15.22");


  // just one log event, for testing
  logging_sink::LogEventBatchRequest request;
  logging_sink::LogEventBatchResponse response;
  auto log_event = request.add_log_events();
  log_event->set_json_line(json_log_entry);
  log_event->set_level(::logging_sink::LOG_LEVEL::LL_WARN);
  log_event->set_service("fivetran-connector");
  //log_event->set_line(json_log_entry);

  std::cout << "*** actual json: [" << json_log_entry << "]" << std::endl;

  const auto result = client->LogEventBatch(context.get(), request, &response);
  std::cout << "****** result of grpc log batch call: "
      << result.ok() << " (error code = " << to_string(result.error_code()) + ")"
      << " (error message = " << result.error_message() + ")";
}

void MdLog::info(const std::string& message) { log( "INFO", message); }

void MdLog::warning(const std::string &message) { log( "WARNING", message); }

void MdLog::severe(const std::string &message) { log("SEVERE", message); }
} // namespace mdlog