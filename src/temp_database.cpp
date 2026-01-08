#include "temp_database.hpp"
#include "md_logging.hpp"
#include <memory>
#include <string>

TempDatabase::TempDatabase(duckdb::Connection &_con,
                           mdlog::Logger &_logger)
    : con(_con), logger(_logger) {
  const auto con_id = con.context->GetConnectionId();
  name = "temp_mem_db_" + std::to_string(con_id);

  // Run DETACH just to be extra sure we don't run into conflicts
  con.Query("DETACH DATABASE IF EXISTS " + name);
  const auto attach_res = con.Query("ATTACH ':memory:' AS " + name);
  if (attach_res->HasError()) {
    attach_res->ThrowError("Failed to attach in-memory database \"" + name +
                           "\": ");
  }

  logger.info("    attached temp database " + name);
}

TempDatabase::~TempDatabase() {
  logger.info("    detaching temp database " + name);
  // Only log errors during DETACH, but continue execution
  const auto detach_res = con.Query("DETACH DATABASE IF EXISTS " + name);
  if (detach_res->HasError()) {
    logger.warning("Failed to detach temporary in-memory database \"" + name +
                    "\": " + detach_res->GetError());
  }
}
