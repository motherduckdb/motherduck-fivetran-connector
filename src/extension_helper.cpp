#include "duckdb.hpp"

void preload_extensions() {
  // create an in-memory DuckDB instance
  duckdb::DuckDB db;
  duckdb::Connection con(db);
  {
    // Preinstall the wrapper
    auto result = con.Query("INSTALL motherduck");
    if (result->HasError()) {
      throw std::runtime_error("Could not install motherduck extension prior "
                               "to gRPC server startup");
    }
  }
  {
    // Load the wrapper to preinstall the motherduck extension
    auto result = con.Query("LOAD motherduck");
    if (result->HasError()) {
      throw std::runtime_error(
          "Could not load motherduck extension prior to gRPC server startup");
    }
  }
  {
    // Preinstall core_functions; no need to load as every duckdb instance will
    // do that
    auto result = con.Query("INSTALL core_functions");
    if (result->HasError()) {
      throw std::runtime_error(
          "Could not install core_functions extension prior "
          "to gRPC server startup");
    }
  }
  {
    // Preinstall core_functions; no need to load as every duckdb instance will
    // do that
    // Parquet is needed to enable zstd compression:
    // https://github.com/duckdb/duckdb/blob/c8906e701ea8202fce34813b151933275f501f4b/src/common/virtual_file_system.cpp#L52-54
    auto result = con.Query("INSTALL parquet");
    if (result->HasError()) {
      throw std::runtime_error("Could not install parquet extension prior "
                               "to gRPC server startup");
    }
  }
}
