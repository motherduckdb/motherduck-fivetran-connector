#pragma once

#include "duckdb.hpp"
#include "ingest_properties.hpp"
#include "md_logging.hpp"

#include <functional>
#include <memory>
#include <string>

namespace csv_processor {
/// Creates a table that contains the contents of the CSV file located at
/// `props.filename`, then calls `process_staging_table` with the
/// fully-qualified name of the created table. Lastly, the table is dropped
/// again.
void ProcessFile(duckdb::Connection &con, const IngestProperties &props, mdlog::Logger &logger,
                 const std::function<void(const std::string &staging_table_name)> &process_staging_table);

} // namespace csv_processor
