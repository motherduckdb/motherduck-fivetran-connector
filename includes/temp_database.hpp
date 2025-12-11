#pragma once

#include "duckdb.hpp"
#include "md_logging.hpp"
#include <memory>
#include <string>

/// In-memory database that gets detached when it goes out of scope.
/// Its lifetime must be shorter than the connection it is attached to.
class TempDatabase final {
public:
  explicit TempDatabase(duckdb::Connection &_con,
                        const std::shared_ptr<mdlog::MdLog> &_logger);
  ~TempDatabase();

  std::string name;

private:
  // The lifetime of TempDatabase must be shorter than connection, which is the
  // case in WriteBatch and WriteHistoryBatch
  duckdb::Connection &con;
  std::shared_ptr<mdlog::MdLog> logger;
};
