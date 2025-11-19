#pragma once

#include "duckdb.hpp"
#include "ingest_properties.hpp"
#include "md_logging.hpp"

#include <functional>
#include <memory>
#include <string>

namespace csv_processor {
/// Represents a DuckDB view that returns the contents of a CSV file via an
/// Arrow array stream. Releases the Arrow array stream when it goes out of
/// scope.
class CSVView {
public:
  static CSVView FromArrow(duckdb::DatabaseInstance &_db,
                           ArrowArrayStream &arrow_array_stream,
                           const std::string &filename,
                           std::shared_ptr<mdlog::MdLog> &logger);

  // Deleted copy constructor and copy assignment operator because of ownership
  // of ArrowArrayStream
  CSVView(const CSVView &) = delete;
  CSVView &operator=(const CSVView &) = delete;
  CSVView(CSVView &&other) noexcept;
  CSVView &operator=(CSVView &&other) noexcept;
  ~CSVView();

  [[nodiscard]] std::string GetFullyQualifiedName() const;

private:
  explicit CSVView(duckdb::DatabaseInstance &_db,
                   ArrowArrayStream &_arrow_array_stream,
                   std::shared_ptr<mdlog::MdLog> logger);

  // Owns a database instance to be able to create/destroy temp databases in its
  // constructor and destructor
  duckdb::DuckDB db;
  ArrowArrayStream arrow_array_stream;
  std::shared_ptr<mdlog::MdLog> logger;

  std::string catalog;
  std::string schema;
  std::string view_name;
};

/// Creates a DuckDB view that returns the contents of the CSV file located at
/// `props.filename`. Returns the fully-qualified name of the created view.
CSVView CreateCSVViewFromFile(const duckdb::Connection &con,
                              const IngestProperties &props,
                              std::shared_ptr<mdlog::MdLog> &logger);

/// Creates a DuckDB view that returns the contents of the CSV file located at
/// `props.filename`, then calls `process_view` with the fully-qualified name of
/// the created view.
void ProcessFile(
    const duckdb::Connection &con, const IngestProperties &props,
    std::shared_ptr<mdlog::MdLog> &logger,
    const std::function<void(const std::string &view_name)> &process_view);

} // namespace csv_processor