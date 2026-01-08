#pragma once

#include "connection_factory.hpp"
#include "config.hpp"
#include "md_logging.hpp"
#include "duckdb.hpp"
#include "google/protobuf/map.h"

#include <string>

/// Context for a single request to the MotherDuck destination server.
/// Contains the DuckDB connection and logger for the request.
class RequestContext {
public:
    explicit RequestContext(const std::string &endpoint_name_, ConnectionFactory &connection_factory,
                            const google::protobuf::Map<std::string, std::string>
                                &request_config) : endpoint_name(endpoint_name_);
    ~RequestContext();

    /// Get the DuckDB connection for the current request
    duckdb::Connection &get_connection() { return con; }
    /// Get the logger for the current request
    mdlog::Logger &get_logger() { return logger; }

private:
    std::string endpoint_name;
    duckdb::Connection con;
    // Logger has to have shorter lifetime than the connection
    mdlog::Logger logger;
    // TODO: SQLGenerator sql_generator;
};