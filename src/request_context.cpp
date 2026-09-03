#include "request_context.hpp"

#include "config.hpp"
#include "connection_factory.hpp"
#include "google/protobuf/map.h"

#include <cstdlib>
#include <mutex>
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

std::string read_env(const char* name) {
	const char* value = std::getenv(name);
	if (value == nullptr || *value == '\0') {
		return "unknown";
	}
	return value;
}

// FIVETRAN_ACCOUNT_NAME and FIVETRAN_GROUP_NAME are set by Fivetran
void log_fivetran_user_info_once(const mdlog::Logger& logger, const std::string& db_name) {
	static std::once_flag user_info_logged;
	std::call_once(user_info_logged, [&logger, &db_name]() {
		logger.debug("Fivetran session started. fivetran_account=<" + read_env("FIVETRAN_ACCOUNT_NAME") +
		             ">, fivetran_group=<" + read_env("FIVETRAN_GROUP_NAME") + ">, database=<" + db_name + ">");
	});
}

} // namespace

RequestContext::RequestContext(const std::string& endpoint_name_, ConnectionFactory& connection_factory,
                               const google::protobuf::Map<std::string, std::string>& request_config)
    : endpoint_name(endpoint_name_), db_name(config::find_property(request_config, config::PROP_DATABASE)),
      md_token(config::find_property(request_config, config::PROP_TOKEN)),
      con(connection_factory.CreateConnection(md_token, db_name)), logger(get_logger_for_env(con)),
      started_at(std::chrono::steady_clock::now()) {
	log_fivetran_user_info_once(logger, db_name);
	logger.debug("Endpoint <" + endpoint_name + "> started");
}

RequestContext::~RequestContext() {
	const auto elapsed_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started_at).count();

	// A destructor is implicitly noexcept, so anything thrown here terminates the
	// process and is reported as a crash. Rollback goes through Query("ROLLBACK")
	// and throws when the connection is no longer usable, which is a situation we need to handle.
	try {
		if (con.HasActiveTransaction() && !con.IsAutoCommit()) {
			con.Rollback();
		}
	} catch (const std::exception& ex) {
		logger.warning("Endpoint <" + endpoint_name + ">: rollback failed during cleanup: " + std::string(ex.what()));
	} catch (...) {
		logger.warning("Endpoint <" + endpoint_name + ">: rollback failed during cleanup with an unknown exception");
	}

	logger.debug("Endpoint <" + endpoint_name + "> completed in " + std::to_string(elapsed_ms) + "ms");
}