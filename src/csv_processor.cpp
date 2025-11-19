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
CSVView CSVView::FromArrow(duckdb::DatabaseInstance &_db,
                           ArrowArrayStream &arrow_array_stream,
                           const std::string &filename,
                           std::shared_ptr<mdlog::MdLog> &logger) {
  duckdb::Connection con(_db);

  const auto con_id = con.context->GetConnectionId();
  const auto temp_db_name = "temp_mem_db_" + std::to_string(con_id);

  // Run DETACH just to be extra sure
  con.Query("DETACH DATABASE IF EXISTS " + temp_db_name);
  con.Query("ATTACH ':memory:' AS " + temp_db_name);
  // Use local memory by default to prevent Arrow-based VIEW from traveling
  // up to the cloud
  con.Query("USE " + temp_db_name);

  auto c_con = reinterpret_cast<duckdb_connection>(&con);
  // TODO: Call duckdb_destroy_arrow_stream?
  auto c_arrow_stream = (duckdb_arrow_stream)&arrow_array_stream;
  logger->info("    duckdb_arrow_stream created for file " + filename);
  duckdb_arrow_scan(c_con, "arrow_view", c_arrow_stream);
  logger->info("    duckdb_arrow_scan completed for file " + filename);

  CSVView csv_view(_db, arrow_array_stream, logger);
  csv_view.catalog = temp_db_name;
  csv_view.schema = "main";
  csv_view.view_name = "arrow_view";

  return csv_view;
}

// Takes an ArrowArrayStream by reference, then copies it into the
// arrow_array_stream member. On the original ArrowArrayStream, sets release to
// nullptr to prevent double free.
CSVView::CSVView(duckdb::DatabaseInstance &_db,
                 ArrowArrayStream &_arrow_array_stream,
                 std::shared_ptr<mdlog::MdLog> _logger)
    : db(_db), arrow_array_stream(_arrow_array_stream),
      logger(std::move(_logger)) {
  _arrow_array_stream.release = nullptr;
}

CSVView::CSVView(CSVView &&other) noexcept
    : db(*other.db.instance), arrow_array_stream(other.arrow_array_stream),
      logger(std::move(other.logger)), catalog(std::move(other.catalog)),
      schema(std::move(other.schema)), view_name(std::move(other.view_name)) {
  other.arrow_array_stream.release = nullptr;
}

CSVView &CSVView::operator=(CSVView &&other) noexcept {
  if (this != &other) {
    db = duckdb::DuckDB(*other.db.instance);
    arrow_array_stream = other.arrow_array_stream;
    other.arrow_array_stream.release = nullptr;
    logger = std::move(other.logger);
    catalog = std::move(other.catalog);
    schema = std::move(other.schema);
    view_name = std::move(other.view_name);
  }
  return *this;
}

CSVView::~CSVView() {
  logger->info("    Detaching temp database " + catalog + " for CSV view");
  duckdb::Connection con(db);
  con.Query("DETACH DATABASE IF EXISTS " + catalog);
  logger->info("    Releasing Arrow array stream");
  arrow_array_stream.release(&arrow_array_stream);
}

std::string CSVView::GetFullyQualifiedName() const {
  return "\"" + catalog + "\".\"" + schema + "\".\"" + view_name + "\"";
}

CSVView CreateCSVViewFromFile(const duckdb::Connection &con,
                              const IngestProperties &props,
                              std::shared_ptr<mdlog::MdLog> &logger) {
  validate_file(props.filename);
  logger->info("    validated file " + props.filename);
  const auto table = props.decryption_key.empty() ? read_unencrypted_csv(props)
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

  auto view = CSVView::FromArrow(*con.context->db, arrow_array_stream,
                                 props.filename, logger);
  if (arrow_array_stream.release != nullptr) {
    throw std::runtime_error(
        "Arrow array stream release function was not consumed for file <" +
        props.filename + ">");
  }
  return view;
}

void ProcessFile(
    const duckdb::Connection &con, const IngestProperties &props,
    std::shared_ptr<mdlog::MdLog> &logger,
    const std::function<void(const std::string &view_name)> &process_view) {
  const auto view = CreateCSVViewFromFile(con, props, logger);
  logger->info("    created CSV view for file " + props.filename);
  process_view(view.GetFullyQualifiedName());
  logger->info("    view processed for file " + props.filename);
}
} // namespace csv_processor