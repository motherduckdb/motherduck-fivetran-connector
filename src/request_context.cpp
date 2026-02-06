#include "request_context.hpp"
#include "config.hpp"
#include "connection_factory.hpp"
#include "google/protobuf/map.h"

#include <cstdlib>
#include <limits>
#include <string>

RequestConfiguration RequestConfiguration::FromProtoMap(
    const google::protobuf::Map<std::string, std::string> &request_config) {
    RequestConfiguration config;
    config.motherduck_token = config::find_property(request_config, config::PROP_TOKEN);
    config.motherduck_database = config::find_property(request_config, config::PROP_DATABASE);

    const auto csv_max_line_size = config::find_optional_property(request_config, config::PROP_CSV_MAX_LINE_SIZE);
    if (csv_max_line_size.has_value()) {
        int numeric_input;
        try {
            numeric_input = std::stoi(csv_max_line_size.value());
        } catch (std::invalid_argument &ex) {
            throw std::invalid_argument("Invalid value for property " + std::string(config::PROP_CSV_MAX_LINE_SIZE) + ": " + csv_max_line_size.value() + ". Must be an unsigned integer.");
        }
        if (numeric_input <= 0) {
            throw std::invalid_argument("Invalid value for property " + std::string(config::PROP_CSV_MAX_LINE_SIZE) + ": " + csv_max_line_size.value() + ". Must be greater than 0.");
        }
        constexpr std::uint32_t max_mib = std::numeric_limits<std::uint32_t>::max() / 1024 / 1024;
        if (numeric_input > static_cast<int>(max_mib)) {
            throw std::invalid_argument("Invalid value for property " + std::string(config::PROP_CSV_MAX_LINE_SIZE) + ": " + csv_max_line_size.value() + ". Must be less than or equal to " + std::to_string(max_mib) + ".");
        }
        const auto input_bytes = static_cast<std::uint32_t>(numeric_input) * 1024 * 1024;
        if (input_bytes < config::DEFAULT_CSV_MAX_LINE_SIZE_BYTES) {
            // Silently ignore values that are smaller than default
            config.csv_max_line_size = config::DEFAULT_CSV_MAX_LINE_SIZE_BYTES
        } else {
            config.csv_max_line_size = input_bytes;
        }
    }

}

RequestContext::RequestContext(
    const std::string &endpoint_name_, ConnectionFactory &connection_factory,
    const google::protobuf::Map<std::string, std::string> &request_config)
    : endpoint_name(endpoint_name_),
      configuration(RequestConfiguration::FromProtoMap(request_config)),
      con(connection_factory.GetConnection(configuration.motherduck_token, configuration.motherduck_database)),
      logger(std::getenv("MD_DISABLE_DUCKDB_LOGGING")
                 ? mdlog::Logger::CreateStdoutLogger()
                 : mdlog::Logger::CreateMultiSinkLogger(&con)) {
  logger.info("Endpoint <" + endpoint_name + "> started");
}

RequestContext::~RequestContext() {
  logger.info("Endpoint <" + endpoint_name + "> completed");
}