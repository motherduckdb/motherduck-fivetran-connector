#include "duckdb.hpp"

void preload_extensions() {
  // create an in-memory DuckDB instance
  duckdb::DuckDB db;
  duckdb::Connection con(db);
  {
    // Preinstall the wrapper
    auto md_install_res = con.Query("INSTALL motherduck");
    if (md_install_res->HasError()) {
      md_install_res->ThrowError(
          "Could not install motherduck extension during pre-loading: ");
    }
  }
  {
    // Load the wrapper to preinstall the motherduck extension
    const auto md_load_res = con.Query("LOAD motherduck");
    if (md_load_res->HasError()) {
      md_load_res->ThrowError(
          "Could not load motherduck extension during pre-loading: ");
    }
  }
  {
    // Preinstall core_functions. No need to load as every duckdb instance will
    // do that. Should be no-op because core_functions are statically linked.
    const auto core_functions_load_res = con.Query("INSTALL core_functions");
    if (core_functions_load_res->HasError()) {
      core_functions_load_res->ThrowError(
          "Could not install core_functions during pre-loading: ");
    }
  }
  {
    // Parquet is needed to enable zstd compression:
    // https://github.com/duckdb/duckdb/blob/c8906e701ea8202fce34813b151933275f501f4b/src/common/virtual_file_system.cpp#L52-54
    const auto parquet_load_res = con.Query("INSTALL parquet");
    if (parquet_load_res->HasError()) {
      parquet_load_res->ThrowError(
          "Could not install parquet during pre-loading: ");
    }
  }
}
