#pragma once

#include "duckdb.hpp"
#include "schema_types.hpp"
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

void find_primary_keys(
    const std::vector<column_def> &cols,
    std::vector<const column_def *> &columns_pk,
    std::vector<const column_def *> *columns_regular = nullptr,
    const std::string &ignored_primary_key = "");

class MdSqlGenerator {

public:
  explicit MdSqlGenerator(mdlog::Logger &logger_);

  bool schema_exists(duckdb::Connection &con, const std::string &db_name,
                     const std::string &schema_name);

  void create_schema(duckdb::Connection &con, const std::string &db_name,
                     const std::string &schema_name);

  bool table_exists(duckdb::Connection &con, const table_def &table);

  void create_table(duckdb::Connection &con, const table_def &table,
                    const std::vector<column_def> &all_columns,
                    const std::set<std::string> &columns_with_default_value);

  std::vector<column_def> describe_table(duckdb::Connection &con,
                                         const table_def &table);

  void alter_table(duckdb::Connection &con, const table_def &table,
                   const std::vector<column_def> &requested_columns);

  void upsert(duckdb::Connection &con, const table_def &table,
              const std::string &staging_table_name,
              const std::vector<const column_def *> &columns_pk,
              const std::vector<const column_def *> &columns_regular);

  void insert(duckdb::Connection &con, const table_def &table,
              const std::string &staging_table_name,
              const std::vector<const column_def *> &columns_pk,
              const std::vector<const column_def *> &columns_regular);

  void update_values(duckdb::Connection &con, const table_def &table,
                     const std::string &staging_table_name,
                     std::vector<const column_def *> &columns_pk,
                     std::vector<const column_def *> &columns_regular,
                     const std::string &unmodified_string);

  void add_partial_historical_values(
      duckdb::Connection &con, const table_def &table,
      const std::string &staging_table_name,
      std::vector<const column_def *> &columns_pk,
      std::vector<const column_def *> &columns_regular,
      const std::string &unmodified_string, const std::string &temp_db_name);

  void truncate_table(duckdb::Connection &con, const table_def &table,
                      const std::string &synced_column,
                      std::chrono::nanoseconds &cutoff_ns,
                      const std::string &deleted_column);

  void delete_rows(duckdb::Connection &con, const table_def &table,
                   const std::string &staging_table_name,
                   std::vector<const column_def *> &columns_pk);

  void
  deactivate_historical_records(duckdb::Connection &con, const table_def &table,
                                const std::string &staging_table_name,
                                std::vector<const column_def *> &columns_pk,
                                const std::string &temp_db_name);

  void delete_historical_rows(duckdb::Connection &con, const table_def &table,
                              const std::string &staging_table_name,
                              std::vector<const column_def *> &columns_pk);

private:
  mdlog::Logger &logger;

  void run_query(duckdb::Connection &con, const std::string &log_prefix,
                 const std::string &query,
                 const std::string &error_message) const;
  void alter_table_recreate(duckdb::Connection &con, const table_def &table,
                            const std::vector<column_def> &all_columns,
                            const std::set<std::string> &common_columns);
  void
  alter_table_in_place(duckdb::Connection &con,
                       const std::string &absolute_table_name,
                       const std::vector<column_def> &added_columns,
                       const std::set<std::string> &alter_types,
                       const std::map<std::string, column_def> &new_column_map);
};
