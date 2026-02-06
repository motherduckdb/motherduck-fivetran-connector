#pragma once

#include "connection_factory.hpp"
#include "duckdb.hpp"
#include "google/protobuf/map.h"
#include "md_logging.hpp"

#include <cstdint>
#include <optional>
#include <string>

class RequestConfiguration {
public:
    static RequestConfiguration FromProtoMap(
        const google::protobuf::Map<std::string, std::string> &request_config);

    std::string motherduck_token;
    std::string motherduck_database;
    std::optional<std::uint32_t> csv_max_line_size;

private:
    RequestConfiguration() = delete;
};

/// Context for a single request to the MotherDuck destination server.
/// Contains the DuckDB connection and logger for the request.
class RequestContext {
public:
  explicit RequestContext(
      const std::string &endpoint_name_, ConnectionFactory &connection_factory,
      const google::protobuf::Map<std::string, std::string> &request_config);
  ~RequestContext();

    /// TODO Comment
  RequestConfiguration &GetConfiguration() { return configuration; }
  /// Get the DuckDB connection for the current request
  duckdb::Connection &GetConnection() { return con; }
  /// Get the logger for the current request
  mdlog::Logger &GetLogger() { return logger; }

private:
  std::string endpoint_name;
  RequestConfiguration configuration;
  duckdb::Connection con;
  // Logger has to have a shorter lifetime than the connection
  mdlog::Logger logger;
};