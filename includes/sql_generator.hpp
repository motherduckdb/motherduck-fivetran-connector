#pragma once

#include "duckdb.hpp"
#include <md_logging.hpp>

struct column_def {
  std::string name;
  duckdb::LogicalTypeId type;
  bool primary_key;
  unsigned int width;
  unsigned int scale;
};

struct table_def {
  std::string db_name;
  std::string schema_name;
  std::string table_name;

  std::string to_escaped_string() const;
};

class MdSqlGenerator {

public:
    explicit MdSqlGenerator(std::shared_ptr<mdlog::MdLog> &logger_);

bool schema_exists(duckdb::Connection &con, const std::string &db_name,
                   const std::string &schema_name);

void create_schema(duckdb::Connection &con, const std::string &db_name,
                   const std::string &schema_name);

bool table_exists(duckdb::Connection &con, const table_def &table);

void create_table(duckdb::Connection &con, const table_def &table,
                  const std::vector<const column_def *> &columns_pk,
                  const std::vector<column_def> &all_columns);

std::vector<column_def> describe_table(duckdb::Connection &con,
                                       const table_def &table);

void alter_table(duckdb::Connection &con, const table_def &table,
                 const std::vector<column_def> &columns);

void upsert(duckdb::Connection &con, const table_def &table,
            const std::string &staging_table_name,
            const std::vector<const column_def *> &columns_pk,
            const std::vector<const column_def *> &columns_regular);

void update_values(duckdb::Connection &con, const table_def &table,
                   const std::string &staging_table_name,
                   std::vector<const column_def *> &columns_pk,
                   std::vector<const column_def *> &columns_regular,
                   const std::string &unmodified_string);

void truncate_table(duckdb::Connection &con, const table_def &table,
                    const std::string &synced_column,
                    std::chrono::nanoseconds &cutoff_ns,
                    const std::string &deleted_column);

void delete_rows(duckdb::Connection &con, const table_def &table,
                 const std::string &staging_table_name,
                 std::vector<const column_def *> &columns_pk);

void check_connection(duckdb::Connection &con);
private:
    std::shared_ptr<mdlog::MdLog> logger;
};