#include "connection_factory.hpp"

#include "config.hpp"
#include "duckdb.hpp"
#include "md_error.hpp"

#include <exception>
#include <mutex>
#include <stdexcept>
#include <string>

namespace {
void maybe_rewrite_error(const std::exception& ex, const std::string& db_name) {
	const duckdb::ErrorData error(ex);
	const auto& msg = error.Message();

	if (msg.find("Jwt is expired") != std::string::npos) {
		throw md_error::RecoverableError("Failed to connect to MotherDuck database \"" + db_name +
		                                 "\" because your MotherDuck token has expired. Please configure a "
		                                 "new MotherDuck token.\nOriginal error: " +
		                                 msg);
	}

	if (msg.find("Your request is not authenticated") != std::string::npos || // Random JWT token
	    msg.find("Invalid MotherDuck token") != std::string::npos) {          // Revoked token
		throw md_error::RecoverableError("Failed to connect to MotherDuck database \"" + db_name +
		                                 "\" because your MotherDuck token is invalid. Please configure a "
		                                 "new MotherDuck token.\nOriginal error: " +
		                                 msg);
	}
}

// Temporary, targeted diagnostics: raise MotherDuck client logging for a single
// user while their connector crashes are under investigation. Remove once those are understood.
constexpr const char* VERBOSE_LOGGING_USER_ID = "b8e29aae-1ee8-4e24-bb77-a61e15155589";
constexpr const char* VERBOSE_LOG_LEVEL = "debug";

void enable_verbose_logging_for_user(duckdb::Connection& con, const mdlog::Logger& logger) {
	const auto user_res = con.Query("SELECT user_id FROM md_user_info()");
	if (user_res->HasError()) {
		logger.warning("get_duckdb: could not read the user id, leaving the log level unchanged: " +
		               user_res->GetError());
		return;
	}
	if (user_res->RowCount() == 0) {
		return;
	}

	const auto user_id = user_res->GetValue(0, 0).ToString();
	if (user_id != VERBOSE_LOGGING_USER_ID) {
		return;
	}

	const auto set_res = con.Query("SET motherduck_log_level='" + std::string(VERBOSE_LOG_LEVEL) + "'");
	if (set_res->HasError()) {
		logger.warning("get_duckdb: could not raise the MotherDuck log level for user <" + user_id +
		               ">: " + set_res->GetError());
		return;
	}
	logger.info("get_duckdb: raised MotherDuck client logging to '" + std::string(VERBOSE_LOG_LEVEL) + "' for user <" +
	            user_id + ">");
}
} // namespace

duckdb::DuckDB& ConnectionFactory::get_duckdb(const std::string& md_auth_token, const std::string& db_name) {
	auto initialize_db = [this, &md_auth_token, &db_name]() {
		duckdb::DBConfig config;
		config.SetOptionByName(config::PROP_TOKEN, md_auth_token);
		config.SetOptionByName("custom_user_agent", std::string("fivetran/") + GIT_COMMIT_SHA);
		config.SetOptionByName("old_implicit_casting", true);
		config.SetOptionByName("motherduck_attach_mode", "single");

		try {
			stdout_logger.info("get_duckdb: creating database instance");
			db = duckdb::DuckDB("md:" + db_name, &config);
		} catch (std::exception& ex) {
			maybe_rewrite_error(ex, db_name);
			throw;
		}

		duckdb::Connection con(db);
		// Trigger welcome pack fetch, but do not raise errors
		const auto welcome_pack_res = con.Query("FROM md_welcome_messages()");
		if (welcome_pack_res->HasError()) {
			stdout_logger.severe("get_duckdb: Could not fetch welcome pack: " + welcome_pack_res->GetError());
		} else {
			stdout_logger.info("get_duckdb: fetched welcome pack");
		}

		// Set default_collation to a connection-specific default value which
		// overwrites any global setting and ensures that client-side planning and
		// server-side execution use the same collation.
		const auto set_collation_res = con.Query("SET GLOBAL default_collation=''");
		if (set_collation_res->HasError()) {
			stdout_logger.severe("get_duckdb: Could not SET default_collation: " + set_collation_res->GetError());
		}

		enable_verbose_logging_for_user(con, stdout_logger);

		initial_md_token = md_auth_token;
		initial_db_name = db_name;
	};

	std::call_once(db_init_flag, initialize_db);

	if (md_auth_token != initial_md_token) {
		throw std::runtime_error("Trying to connect to MotherDuck with a different "
		                         "token than initially provided");
	}

	if (db_name != initial_db_name) {
		throw std::runtime_error("Trying to connect to a different MotherDuck database (" + db_name +
		                         ") than on the initial connection (" + initial_db_name + ")");
	}

	return db;
}

duckdb::Connection ConnectionFactory::CreateConnection(const std::string& md_auth_token, const std::string& db_name) {
	stdout_logger.info("create_connection: start");
	duckdb::DuckDB& db = get_duckdb(md_auth_token, db_name);
	return duckdb::Connection(db);
}
