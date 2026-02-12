#include "md_logging.hpp"

#include "duckdb.hpp"

#include <iostream>
#include <mutex>
#include <string>

namespace mdlog {

namespace {

Logger::SinkType operator|(Logger::SinkType lhs, Logger::SinkType rhs) {
	return static_cast<Logger::SinkType>(static_cast<int>(lhs) | static_cast<int>(rhs));
}

bool HasFlag(const Logger::SinkType value, const Logger::SinkType flag) {
	return static_cast<Logger::SinkType>(static_cast<int>(value) & static_cast<int>(flag)) != Logger::SinkType::NONE;
}
} // namespace

Logger::Logger(const SinkType sinks) : enabled_sinks(sinks) {
	// The other constructor should be used for DuckDB logging
	assert(!HasFlag(enabled_sinks, SinkType::DUCKDB));
}

Logger::Logger(duckdb::Connection* con_) : enabled_sinks(SinkType::STDOUT | SinkType::DUCKDB), con(con_) {
	assert(con != nullptr);
	const auto client_ids_res = con->Query("SELECT md_current_client_duckdb_id(), "
	                                       "md_current_client_connection_id()");
	if (client_ids_res->HasError()) {
		log_to_stdout("WARNING",
		              "Could not retrieve the current DuckDB and connection ID: " + client_ids_res->GetError());
	} else {
		duckdb_id = client_ids_res->GetValue(0, 0).ToString();
		connection_id = client_ids_res->GetValue(1, 0).ToString();
	}
}

void Logger::log_to_stdout(const std::string& level, const std::string& message) const {
	std::cout << "{\"level\":\"" << duckdb::KeywordHelper::EscapeQuotes(level, '"') << "\",\"message\":\""
	          << duckdb::KeywordHelper::EscapeQuotes(message, '"') << ", duckdb_id=<" << duckdb_id
	          << ">, connection_id=<" << connection_id << ">\",\"message-origin\":\"sdk_destination\"}" << std::endl;
}

void Logger::log_to_duckdb(const std::string& level, const std::string& message) const {
	std::string ddb_log_level;
	if (level == "INFO") {
		ddb_log_level = "INFO";
	} else if (level == "WARNING") {
		ddb_log_level = "WARN";
	} else if (level == "SEVERE") {
		ddb_log_level = "ERROR";
	} else {
		ddb_log_level = "INFO";
	}

	std::string full_message = message;
	duckdb::StringUtil::Trim(full_message);

	const std::string query =
	    "SELECT write_log(" + duckdb::KeywordHelper::WriteQuoted(full_message, '\'') +
	    ", log_type:='Fivetran', level:=" + duckdb::KeywordHelper::WriteQuoted(ddb_log_level, '\'') + ")";

	const auto log_res = con->Query(query);

	// Only log errors from the query, but continue execution
	if (log_res->HasError()) {
		log_to_stdout("WARNING", "Failed to write log to DuckDB: " + log_res->GetError());
	}
}

void Logger::log(const std::string& level, const std::string& message) const {
	if (HasFlag(enabled_sinks, SinkType::STDOUT)) {
		log_to_stdout(level, message);
	}

	if (HasFlag(enabled_sinks, SinkType::DUCKDB)) {
		// enable_logging is a global setting, so it only needs to be called once
		// per DuckDB instance. And the DuckDB instance is a singleton.
		std::call_once(
		    initialize_duckdb_logging_flag,
		    [](duckdb::Connection& con) {
			    con.Query("CALL enable_logging('Fivetran', storage='motherduck_log_storage', level='INFO')");
		    },
		    *con);
		log_to_duckdb(level, message);
	}
}

void Logger::info(const std::string& message) const {
	log("INFO", message);
}

void Logger::warning(const std::string& message) const {
	log("WARNING", message);
}

void Logger::severe(const std::string& message) const {
	log("SEVERE", message);
}

} // namespace mdlog
