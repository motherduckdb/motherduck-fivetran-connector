#include "request_context.hpp"

RequestContext::RequestContext(
    const std::string &endpoint_name_, ConnectionFactory &connection_factory,
    const google::protobuf::Map<std::string, std::string> &request_config)
    : endpoint_name(endpoint_name_),
      con(connection_factory.GetConnection(
          config::find_property(request_config, config::PROP_TOKEN),
          config::find_property(request_config, config::PROP_DATABASE))),
      logger(mdlog::Logger::CreateMultiSinkLogger(&con)) {
  logger.info("Endpoint <" + endpoint_name + "> started");
}

RequestContext::~RequestContext() {
  logger.info("Endpoint <" + endpoint_name + "> completed");
  // TODO: Look for uncaught exceptions and log them
}