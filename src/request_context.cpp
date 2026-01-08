#include "request_context.hpp"

RequestContext::RequestContext(const std::string &endpoint_name_, ConnectionFactory &connection_factory,
                            const google::protobuf::Map<std::string, std::string>
                                &request_config) : endpoint_name(endpoint_name_) {
    const std::string db_name =
        config::find_property(request_config, config::PROP_DATABASE);
    const std::string md_token =
        config::find_property(request_config, config::PROP_TOKEN);

    con = connection_factory.GetConnection(md_token, db_name);
    logger = mdlog::Logger::CreateMultiSinkLogger(con);

    logger.info("Endpoint <" + endpoint_name + "> started");
}

RequestContext::~RequestContext() {
    logger.info("Endpoint <" + endpoint_name + "> completed");
    // TODO: Look for uncaught exceptions and log them
}