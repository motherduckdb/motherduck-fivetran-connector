#pragma once

#include "common.pb.h"
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
  explicit MdSqlGenerator(std::shared_ptr<mdlog::MdLog> &logger_);

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
                   const std::vector<column_def> &requested_columns,
                   bool drop_columns);

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

  // Migration operations
  void drop_table(duckdb::Connection &con, const table_def &table);

  void drop_column_in_history_mode(duckdb::Connection &con,
                                   const table_def &table,
                                   const std::string &column,
                                   const std::string &operation_timestamp);

  void copy_table(duckdb::Connection &con, const table_def &from_table,
                  const table_def &to_table);

  void copy_column(duckdb::Connection &con, const table_def &table,
                   const std::string &from_column,
                   const std::string &to_column);

  void copy_table_to_history_mode(duckdb::Connection &con,
                                  const table_def &from_table,
                                  const table_def &to_table,
                                  const std::string &soft_deleted_column);

  void rename_table(duckdb::Connection &con, const table_def &from_table,
                    const std::string &to_table_name);

  void rename_column(duckdb::Connection &con, const table_def &table,
                     const std::string &from_column,
                     const std::string &to_column);

  void add_column_with_default(duckdb::Connection &con, const table_def &table,
                               const std::string &column,
                               fivetran_sdk::v2::DataType type,
                               const std::string &default_value);
  bool validate_history_table(duckdb::Connection &con,
                              std::string absolute_table_name,
                              std::string quoted_timestamp);

  void add_column_in_history_mode(duckdb::Connection &con,
                                  const table_def &table,
                                  const std::string &column,
                                  fivetran_sdk::v2::DataType type,
                                  const std::string &default_value,
                                  const std::string &operation_timestamp);

  void update_column_value(duckdb::Connection &con, const table_def &table,
                           const std::string &column, const std::string &value);

  void migrate_soft_delete_to_live(duckdb::Connection &con,
                                   const table_def &table,
                                   const std::string &soft_deleted_column);

  void migrate_soft_delete_to_history(duckdb::Connection &con,
                                      const table_def &table,
                                      const std::string &soft_deleted_column);

  void migrate_history_to_soft_delete(duckdb::Connection &con,
                                      const table_def &table,
                                      const std::string &soft_deleted_column);

  void migrate_history_to_live(duckdb::Connection &con, const table_def &table,
                               bool keep_deleted_rows);

  void migrate_live_to_soft_delete(duckdb::Connection &con,
                                   const table_def &table,
                                   const std::string &soft_deleted_column);

  void migrate_live_to_history(duckdb::Connection &con, const table_def &table);

private:
  std::shared_ptr<mdlog::MdLog> logger;

  void run_query(duckdb::Connection &con, const std::string &log_prefix,
                 const std::string &query, const std::string &error_message);
  void alter_table_recreate(duckdb::Connection &con, const table_def &table,
                            const std::vector<column_def> &all_columns,
                            const std::set<std::string> &common_columns);
  void
  alter_table_in_place(duckdb::Connection &con,
                       const std::string &absolute_table_name,
                       const std::vector<column_def> &added_columns,
                       const std::set<std::string> &deleted_columns,
                       const std::set<std::string> &alter_types,
                       const std::map<std::string, column_def> &new_column_map);
};
