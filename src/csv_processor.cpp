#include "csv_processor.hpp"

#include "csv_arrow_ingest.hpp"
#include "duckdb.hpp"
#include "md_logging.hpp"

#include <arrow/c/bridge.h>
#include <fstream>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>

namespace {
void validate_file(const std::string &file_path) {
  std::ifstream fs(file_path.c_str());
  if (fs.good()) {
    fs.close();
    return;
  }
  throw std::invalid_argument("File <" + file_path +
                              "> is missing or inaccessible");
}
} // namespace

namespace csv_processor {
void ProcessFile(
    duckdb::Connection &con, const IngestProperties &props,
    std::shared_ptr<mdlog::MdLog> &logger,
    const std::function<void(const std::string &view_name)> &process_view) {
  validate_file(props.filename);
  logger->info("    validated file " + props.filename);
  auto table = props.decryption_key.empty() ? read_unencrypted_csv(props)
                                            : read_encrypted_csv(props);

  auto batch_reader = std::make_shared<arrow::TableBatchReader>(*table);
  ArrowArrayStream arrow_array_stream;
  auto status =
      arrow::ExportRecordBatchReader(batch_reader, &arrow_array_stream);
  if (!status.ok()) {
    throw std::runtime_error(
        "Could not convert Arrow batch reader to an array stream for file <" +
        props.filename + ">: " + status.message());
  }
  logger->info("    ArrowArrayStream created for file " + props.filename);

  const auto con_id = con.context->GetConnectionId();
  const auto temp_db_name = "temp_mem_db_" + std::to_string(con_id);

  // Run DETACH just to be extra sure
  con.Query("DETACH DATABASE IF EXISTS " + temp_db_name);
  con.Query("ATTACH ':memory:' AS " + temp_db_name);
  // Use local memory by default to prevent Arrow-based VIEW from traveling
  // up to the cloud
  con.Query("USE " + temp_db_name);

  duckdb_connection c_con = reinterpret_cast<duckdb_connection>(&con);
  duckdb_arrow_stream c_arrow_stream = (duckdb_arrow_stream)&arrow_array_stream;
  logger->info("    duckdb_arrow_stream created for file " + props.filename);
  duckdb_state state = duckdb_arrow_scan(c_con, "arrow_view", c_arrow_stream);
  if (state != DuckDBSuccess) {
    throw std::runtime_error("Could not scan Arrow scan for file <" +
                             props.filename + ">");
  }
  logger->info("    duckdb_arrow_scan completed for file " + props.filename);

  process_view("\"" + temp_db_name + "\".\"arrow_view\"");
  logger->info("    view processed for file " + props.filename);

  logger->info("    Detaching temp database " + temp_db_name + " for CSV view");
  con.Query("DETACH DATABASE IF EXISTS " + temp_db_name);
  arrow_array_stream.release(&arrow_array_stream);
}
} // namespace csv_processor