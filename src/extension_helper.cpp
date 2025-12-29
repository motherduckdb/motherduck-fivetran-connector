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

#ifndef NDEBUG
  {
    const std::string query =
        "SELECT extension_name, loaded FROM duckdb_extensions() WHERE "
        "extension_name IN ('core_functions', 'parquet')";
    const auto check_exts_res = con.Query(query);
    if (check_exts_res->HasError()) {
      check_exts_res->ThrowError(
          "Could not check extensions during pre-loading: ");
    }

    if (check_exts_res->RowCount() != 2) {
      throw duckdb::InternalException(
          "Expected core_functions and parquet extensions to be loaded, but "
          "not all extensions were found");
    }

    for (idx_t row = 0; row < check_exts_res->RowCount(); row++) {
      const auto ext_name =
          check_exts_res->GetValue(0, row).GetValue<std::string>();
      const auto is_loaded = check_exts_res->GetValue(1, row).GetValue<bool>();
      if (!is_loaded) {
        throw duckdb::InternalException(
            "Expected %s extension to be loaded, but it is not", ext_name);
      }
    }
  }
#endif
}
