#pragma once

#include "duckdb.hpp"

#include <mutex>
#include <string>

namespace mdlog {

class Logger {
public:
	enum class SinkType { NONE = 0, STDOUT = 1 << 0, DUCKDB = 1 << 1 };

	/// Creates a logger that does nothing on `log` calls
	static Logger CreateNopLogger() {
		return Logger(SinkType::NONE);
	}

	/// Creates a logger that logs to stdout only
	static Logger CreateStdoutLogger() {
		return Logger(SinkType::STDOUT);
	}

	// Creates a logger that logs to both stdout and DuckDB
	static Logger CreateMultiSinkLogger(duckdb::Connection* connection) {
		return Logger(connection);
	}

	void log(const std::string& level, const std::string& message) const;
	void info(const std::string& message) const;
	void warning(const std::string& message) const;
	void severe(const std::string& message) const;

private:
	// Used for NopLogger and StdoutLogger
	explicit Logger(SinkType sinks);

	// Logs to both stdout and DuckDB
	explicit Logger(duckdb::Connection* con_);

	SinkType enabled_sinks = SinkType::NONE;
	// This is a raw pointer because it can be optional. The Logger is created as
	// part of the RequestContext which ensures that the duckdb::Connection
	// outlives the Logger.
	duckdb::Connection* con;
	std::string duckdb_id = "none";
	std::string connection_id = "none";
	mutable std::once_flag initialize_duckdb_logging_flag;

	void log_to_stdout(const std::string& level, const std::string& message) const;
	void log_to_duckdb(const std::string& level, const std::string& message) const;
};
} // namespace mdlog
