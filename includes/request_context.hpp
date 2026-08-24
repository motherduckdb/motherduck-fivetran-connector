#pragma once

#include "connection_factory.hpp"
#include "duckdb.hpp"
#include "google/protobuf/map.h"
#include "md_logging.hpp"
#include "schema_types.hpp"

#include <string>

/// Context for a single request to the MotherDuck destination server.
/// Contains the DuckDB connection and logger for the request.
class RequestContext {
public:
	explicit RequestContext(const std::string& endpoint_name_, ConnectionFactory& connection_factory,
	                        const google::protobuf::Map<std::string, std::string>& request_config);
	~RequestContext();

	/// Get the DuckDB connection for the current request
	duckdb::Connection& GetConnection() {
		return con;
	}
	/// Get the logger for the current request
	mdlog::Logger& GetLogger() {
		return logger;
	}
	/// Get the name of the target database for the current request
	const std::string& GetDBName() const {
		return db_name;
	}
	/// How primary keys should be enforced for the current request, as configured
	/// by the "Strict Primary Keys" property
	PrimaryKeyMode GetPrimaryKeyMode() const {
		return pk_mode;
	}

private:
	std::string endpoint_name;
	std::string db_name;
	std::string md_token;
	PrimaryKeyMode pk_mode;
	duckdb::Connection con;
	// Logger has to have a shorter lifetime than the connection
	mdlog::Logger logger;
};