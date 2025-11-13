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
                               "to gRPC server startup: " +
                               result->GetError());
    }
  }
  {
    // Load the wrapper to preinstall the motherduck extension
    auto result = con.Query("LOAD motherduck");
    if (result->HasError()) {
      throw std::runtime_error(
          "Could not load motherduck extension prior to gRPC server startup: " +
          result->GetError());
    }
  }
  {
    // Preinstall core_functions; no need to load as every duckdb instance will
    // do that
    auto result = con.Query("INSTALL core_functions");
    if (result->HasError()) {
      throw std::runtime_error(
          "Could not install core_functions extension prior "
          "to gRPC server startup: " +
          result->GetError());
    }
  }
}
