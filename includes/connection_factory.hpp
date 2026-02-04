#pragma once

#include "duckdb.hpp"
#include "md_logging.hpp"

#include <mutex>
#include <string>

/// Used to create a new DuckDB connection to the specified MotherDuck database.
/// In practice, only one db_name is always passed for the entire lifetime of
/// the process. If no duckdb::DuckDB has been instantiated yet, it will create
/// one on the first call to GetConnection. One ConnectionFactory is used per
/// gRPC request.
class ConnectionFactory {
public:
	explicit ConnectionFactory() : stdout_logger(mdlog::Logger::CreateStdoutLogger()) {
	}

	duckdb::Connection CreateConnection(const std::string &md_auth_token, const std::string &db_name);

private:
	duckdb::DuckDB &get_duckdb(const std::string &md_auth_token, const std::string &db_name);

	// Only logs to stdout because there is no duckdb::Connection yet for
	// SQL-based logging
	mdlog::Logger stdout_logger;
	std::once_flag db_init_flag;
	duckdb::DuckDB db;
	// Used to check that the same parameters are used on subsequent calls to
	// GetConnection
	std::string initial_md_token;
	std::string initial_db_name;
};
