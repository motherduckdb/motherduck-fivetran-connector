#pragma once
#include "duckdb.hpp"
#include "md_logging.hpp"

#include <memory>

namespace csv_reader {
struct CSVView {
  // TODO: Take ownership of connection?
  explicit CSVView(duckdb::Connection &con, FileHandle memory_backed_file);
            : connection(con) {
                view_name = "csv_view_" + "some_random_string";
        }

            ~CSVView() {
              // Drop view from DuckDB, potentially detach in-memory database
              // Clean up file descriptor
            }

            // TODO: const??
            // TODO: Change type to duckdb::SQLIdentifier?
            /// Fully-qualified name of the DuckDB view that reads from the CSV
            /// file.
            const std::string name;

          private:
            duckdb::Connection &connection;
            FileHandle memory_backed_file;
};

/// Encrypts the CSV file with the given decryption key and stores the
/// plaintext content in a memory-backed file.
/// Returns the name of a DuckDB view that reads all columns from this CSV file.
CSVView create_view_from_encrypted_csv(duckdb::Connection &con,
                                       IngestProperties &props,
                                       std::shared_ptr<mdlog::MdLog> &logger);
} // namespace csv_reader