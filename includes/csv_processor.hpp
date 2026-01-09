#pragma once

#include "duckdb.hpp"
#include "ingest_properties.hpp"
#include "md_logging.hpp"

#include <functional>
#include <memory>
#include <string>

namespace csv_processor {
/// Creates a DuckDB view that returns the contents of the CSV file located at
/// `props.filename`, then calls `process_view` with the fully-qualified name of
/// the created view.
void ProcessFile(
    duckdb::Connection &con, const IngestProperties &props,
    const mdlog::Logger &logger,
    const std::function<void(const std::string &view_name)> &process_view);

} // namespace csv_processor