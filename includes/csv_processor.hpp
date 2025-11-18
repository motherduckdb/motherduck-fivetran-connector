#pragma once

#include "duckdb.hpp"
#include "ingest_properties.hpp"
#include "md_logging.hpp"

#include <functional>
#include <memory>
#include <string>

namespace csv_processor {

    /// Creates a DuckDB view that returns the contents of the CSV file located at `filepath`.
    /// Returns the fully-qualified name of the created view.
    void process_file(
    duckdb::Connection &con, const IngestProperties &props,
    std::shared_ptr<mdlog::MdLog> &logger,
    const std::function<void(const std::string &view_name)> &process_view);

} // namespace csv_processor