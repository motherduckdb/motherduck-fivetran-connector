#pragma once

#include "duckdb.hpp"
#include "ingest_properties.hpp"
#include "md_logging.hpp"

#include <functional>
#include <memory>
#include <string>

namespace csv_processor {
    class CSVView {
    public:
        static CSVView FromArrow(duckdb::DatabaseInstance &_db, ArrowArrayStream &arrow_array_stream, const std::string &filename, std::shared_ptr<mdlog::MdLog> &logger);

        ~CSVView();

        std::string GetFullyQualifiedName() const;

        // TODO: Move constructor

    private:
        explicit CSVView(duckdb::DatabaseInstance &_db, ArrowArrayStream &_arrow_array_stream);

        // Owns a database instance to be able to create/destroy temp databases in its constructor and destructor
        duckdb::DuckDB db;
        ArrowArrayStream arrow_array_stream;

        std::string catalog;
        std::string schema;
        std::string view_name;
    };

    /// Creates a DuckDB view that returns the contents of the CSV file located at `filepath`.
    /// Returns the fully-qualified name of the created view.
    void process_file(
    duckdb::Connection &con, const IngestProperties &props,
    std::shared_ptr<mdlog::MdLog> &logger,
    const std::function<void(const std::string &view_name)> &process_view);

} // namespace csv_processor