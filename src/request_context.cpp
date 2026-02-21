#include "request_context.hpp"

#include "config.hpp"
#include "connection_factory.hpp"
#include "google/protobuf/map.h"

#include <cstdlib>
#include <string>
#include <string_view>

namespace {
mdlog::Logger get_logger_for_env(duckdb::Connection& con) {
	const char* env_var = std::getenv("MD_DISABLE_DUCKDB_LOGGING");
	if (env_var && std::string_view(env_var) != "0") {
		return mdlog::Logger::CreateStdoutLogger();
	}
	return mdlog::Logger::CreateMultiSinkLogger(&con);
}
} // namespace

RequestContext::RequestContext(const std::string& endpoint_name_, ConnectionFactory& connection_factory,
                               const google::protobuf::Map<std::string, std::string>& request_config)
    : endpoint_name(endpoint_name_),
      con(connection_factory.CreateConnection(config::find_property(request_config, config::PROP_TOKEN),
                                              config::find_property(request_config, config::PROP_DATABASE))),
      logger(get_logger_for_env(con)) {
	logger.info("Endpoint <" + endpoint_name + "> started");
}

RequestContext::~RequestContext() {
	// At this point, this is defensive programming since in theory the transaction contexts manage rollbacks in case of
	// an exception.
	if (con.HasActiveTransaction() && !con.IsAutoCommit()) {
		con.Rollback();
	}
	logger.info("Endpoint <" + endpoint_name + "> completed");
}