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
                           ArrowArrayStream *arrow_stream,
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
  auto c_arrow_stream = reinterpret_cast<duckdb_arrow_stream>(arrow_stream);
  logger->info("    duckdb_arrow_stream created for file " + filename);
  duckdb_state state = duckdb_arrow_scan(c_con, "arrow_view", c_arrow_stream);
  if (state != DuckDBSuccess) {
    throw std::runtime_error(
        "Could not scan Arrow scan for file <" + filename + ">");
  }
  logger->info("    duckdb_arrow_scan completed for file " + filename);

  CSVView csv_view(_db, c_arrow_stream, logger);
  csv_view.catalog = temp_db_name;
  csv_view.schema = "main";
  csv_view.view_name = "arrow_view";

  return csv_view;
}

// Takes an ArrowArrayStream by reference, then copies it into the
// arrow_array_stream member. On the original ArrowArrayStream, sets release to
// nullptr to prevent double free.
CSVView::CSVView(duckdb::DatabaseInstance &_db,
                 duckdb_arrow_stream _arrow_stream,
                 std::shared_ptr<mdlog::MdLog> _logger)
    : db(_db), arrow_stream(_arrow_stream), logger(std::move(_logger)) {
}

CSVView::CSVView(CSVView &&other) noexcept
    : db(*other.db.instance), arrow_stream(other.arrow_stream), logger(std::move(other.logger)), catalog(std::move(other.catalog)),
      schema(std::move(other.schema)), view_name(std::move(other.view_name)) {
  other.arrow_stream = nullptr;
}

CSVView &CSVView::operator=(CSVView &&other) noexcept {
  if (this != &other) {
    // Clean up existing arrow_stream before overwriting
    auto stream = reinterpret_cast<ArrowArrayStream *>(arrow_stream);
    if (stream) {
      printf("!!! Relesae A\n");
      if (stream->release) {
        stream->release(stream);
      }
      delete stream;
    }

    db = duckdb::DuckDB(*other.db.instance);
    arrow_stream = other.arrow_stream;
    other.arrow_stream = nullptr;
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

  printf("!!! Relesae B\n");
  auto stream = reinterpret_cast<ArrowArrayStream *>(arrow_stream);
  if (!stream) {
    return;
  }
  if (stream->release) {
    stream->release(stream);
  }
  D_ASSERT(!stream->release);

  delete stream;
  arrow_stream = nullptr;
}

std::string CSVView::GetCatalog() const {
  return catalog;
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
  auto arrow_array_stream = new ArrowArrayStream();
  auto status =
      arrow::ExportRecordBatchReader(batch_reader, arrow_array_stream);
  if (!status.ok()) {
    delete arrow_array_stream;
    throw std::runtime_error(
        "Could not convert Arrow batch reader to an array stream for file <" +
        props.filename + ">: " + status.message());
  }
  logger->info("    ArrowArrayStream created for file " + props.filename);

  auto view = CSVView::FromArrow(*con.context->db, arrow_array_stream,
                                 props.filename, logger);
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