#pragma once

#include "duckdb.hpp"
#include "md_logging.hpp"
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

  /// Generates a randomized table name which is not used yet in the database
  std::string generate_temp_table_name(duckdb::Connection &con,
                                       const std::string &prefix) const;

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
  void add_column(duckdb::Connection &con, const table_def &table,
                  const column_def &column, const std::string &log_prefix,
                  bool exists_ok = false) const;

  void drop_column(duckdb::Connection &con, const table_def &table,
                   const std::string &column_name,
                   const std::string &log_prefix,
                   bool not_exists_ok = false) const;

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

  /// This creates the latest_active_records (LAR) table, a table with a
  /// randomized name. The caller is responsible for cleaning it up. The LAR
  /// table is used in history mode (see DestinationSdkImpl::WriteHistoryBatch).
  std::string
  create_latest_active_records_table(duckdb::Connection &con,
                                     const table_def &source_table) const;

  void add_partial_historical_values(
      duckdb::Connection &con, const table_def &table,
      const std::string &staging_table_name, const std::string &lar_table_name,
      std::vector<const column_def *> &columns_pk,
      std::vector<const column_def *> &columns_regular,
      const std::string &unmodified_string) const;

  void truncate_table(duckdb::Connection &con, const table_def &table,
                      const std::string &synced_column,
                      std::chrono::nanoseconds &cutoff_ns,
                      const std::string &deleted_column);

  void delete_rows(duckdb::Connection &con, const table_def &table,
                   const std::string &staging_table_name,
                   std::vector<const column_def *> &columns_pk);

  void deactivate_historical_records(
      duckdb::Connection &con, const table_def &table,
      const std::string &staging_table_name, const std::string &lar_table_name,
      std::vector<const column_def *> &columns_pk) const;

  void delete_historical_rows(duckdb::Connection &con, const table_def &table,
                              const std::string &staging_table_name,
                              std::vector<const column_def *> &columns_pk);

  // Migration operations

  // Drop the destination table
  void drop_table(duckdb::Connection &con, const table_def &table,
                  const std::string &log_prefix);

  // In history mode, instead of dropping the actual column we pretend that all
  // column values have been set to NULL in the source. This means that for all
  // rows where the column was not NULL yet, we insert new historic entries
  // where we change the value to NULL and always insert NULL for the column for
  // that point onward.
  //
  // Note: if two columns were dropped at the same time in the source, we get
  // two separate DROP_COLUMN requests with the same operation_timestamp.
  void drop_column_in_history_mode(duckdb::Connection &con,
                                   const table_def &table,
                                   const std::string &column,
                                   const std::string &operation_timestamp);

  // Copy a table in the destination.
  void copy_table(duckdb::Connection &con, const table_def &from_table,
                  const table_def &to_table, const std::string &log_prefix,
                  const std::vector<const column_def *> &additional_pks = {});

  // Copy a column in the destination.
  void copy_column(duckdb::Connection &con, const table_def &table,
                   const std::string &from_column,
                   const std::string &to_column);

  // For a table that is in either in live- or soft-delete-mode, copy it into a
  // new table in history mode. For soft-delete-mode, in which case
  // soft_deleted_column is not empty, we try to retain historic info as much as
  // we can.
  void copy_table_to_history_mode(duckdb::Connection &con,
                                  const table_def &from_table,
                                  const table_def &to_table,
                                  const std::string &soft_deleted_column);

  // Rename a destination table
  void rename_table(duckdb::Connection &con, const table_def &from_table,
                    const std::string &to_table_name,
                    const std::string &log_prefix);

  // Rename a destination column
  void rename_column(duckdb::Connection &con, const table_def &table,
                     const std::string &from_column,
                     const std::string &to_column);

  // Verify the state of the history table before performing schema migrations
  static bool history_table_is_valid(duckdb::Connection &con,
                                     const std::string &absolute_table_name,
                                     const std::string &quoted_timestamp);

  // Add a column in history mode, which means we copy all active tables over to
  // new historic entries with the new column set to the default value, and
  // invalidate old historic entries (where we set the value to NULL).
  void add_column_in_history_mode(duckdb::Connection &con,
                                  const table_def &table,
                                  const column_def &column,
                                  const std::string &operation_timestamp);

  // Update the value of a column of every row
  void update_column_value(duckdb::Connection &con, const table_def &table,
                           const std::string &column, const std::string &value);

  // Switch between sync modes: soft-delete to live. Here this means that we
  // should drop the soft-deleted rows and remove the soft-deleted column (the
  // column used to determine if a row is soft-deleted, often
  // "_fivetran_deleted" unless the source defines its own column).
  void migrate_soft_delete_to_live(duckdb::Connection &con,
                                   const table_def &table,
                                   const std::string &soft_deleted_column);

  // Switch between sync modes: soft-delete to history. Here this means we use
  // the soft-deleted column to determine the value of "_fivetran_active" (i.e.
  // the inverse of the soft-deleted column). The fivetran start/end columns are
  // set to the epoch for deleted rows (we don't know when they were deleted).
  // They are set to MAX(\"_fivetran_synced\") and the maximum possible
  // timestamp respectively for active rows, because we interpret the latest
  // sync as the initial insert into the historic table.
  void migrate_soft_delete_to_history(duckdb::Connection &con,
                                      const table_def &table,
                                      const std::string &soft_deleted_column);
  void add_defaults(duckdb::Connection &con,
                    const std::vector<column_def> &columns,
                    const std::string &table_name,
                    const std::string &log_prefix);
  void add_pks(duckdb::Connection &con,
               const std::vector<const column_def *> &columns_pk,
               const std::string &table_name,
               const std::string &log_prefix) const;

  // Switch between sync modes: history to soft-delete. This means keeping only
  // the last entries based on per MAX("_fivetran_start") per primary key,
  // setting the soft_deleted_column values to "NOT _fivetran_active" and
  // dropping the unused history mode columns.
  void migrate_history_to_soft_delete(duckdb::Connection &con,
                                      const table_def &table,
                                      const std::string &soft_deleted_column);

  // Switch between sync modes: history to live. This means only keeping the
  // rows that are active as indicated by
  // "_fivetran_active".
  void migrate_history_to_live(duckdb::Connection &con, const table_def &table,
                               bool keep_deleted_rows);

  // Switch between sync modes: live to soft-delete. In general live-mode does
  // not keep track of deletions, in which case just adds we want to set the
  // soft-deleted column to False for all rows. But the soft-deleted column
  // might have been defined in the source already, in which case we can take
  // advantage of that. Hence, the implementation adds the column unless it
  // exists and only updates values where the column is NULL.
  void migrate_live_to_soft_delete(duckdb::Connection &con,
                                   const table_def &table,
                                   const std::string &soft_deleted_column);

  // Switch between sync modes: live to history. We do not consider the
  // possibility that a soft-deleted column was already defined here. Hence,
  // this just adds the needed history-mode columns and sets sane defaults: all
  // rows are active, _fivetran_start = NOW() and _fivetran_end is the maximum
  // timestamp possible.
  void migrate_live_to_history(duckdb::Connection &con, const table_def &table);

private:
  mdlog::Logger &logger;

  void run_query(duckdb::Connection &con, const std::string &log_prefix,
                 const std::string &query,
                 const std::string &error_message) const;
  void alter_table_recreate(duckdb::Connection &con, const table_def &table,
                            const std::vector<column_def> &all_columns,
                            const std::set<std::string> &common_columns);
  void
  alter_table_in_place(duckdb::Connection &con, const table_def &table,
                       const std::vector<column_def> &added_columns,
                       const std::set<std::string> &deleted_columns,
                       const std::set<std::string> &alter_types,
                       const std::map<std::string, column_def> &new_column_map);
};
