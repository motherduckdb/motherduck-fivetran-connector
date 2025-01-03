#include <iostream>
#include <md_logging.hpp>
#include <string>

#include "../includes/sql_generator.hpp"

using duckdb::KeywordHelper;

// Utility

std::string table_def::to_escaped_string() const {
  std::ostringstream out;
  out << KeywordHelper::WriteQuoted(db_name, '"') << "."
      << KeywordHelper::WriteQuoted(schema_name, '"') << "."
      << KeywordHelper::WriteQuoted(table_name, '"');
  return out.str();
}

const auto print_column = [](const std::string &quoted_col,
                             std::ostringstream &out) { out << quoted_col; };

void write_joined(
    std::ostringstream &sql, const std::vector<const column_def *> &columns,
    std::function<void(const std::string &, std::ostringstream &)> print_str,
    const std::string &separator = ", ") {
  bool first = true;
  for (const auto &col : columns) {
    if (first) {
      first = false;
    } else {
      sql << separator;
    }
    print_str(KeywordHelper::WriteQuoted(col->name, '"'), sql);
  }
}

void find_primary_keys(const std::vector<column_def> &cols,
                       std::vector<const column_def *> &columns_pk,
                       std::vector<const column_def *> *columns_regular) {
  for (auto &col : cols) {
    if (col.primary_key) {
      columns_pk.push_back(&col);
    } else if (columns_regular != nullptr) {
      columns_regular->push_back(&col);
    }
  }
}

std::string
make_full_column_list(const std::vector<const column_def *> &columns_pk,
                      const std::vector<const column_def *> &columns_regular) {
  std::ostringstream full_column_list;
  if (!columns_pk.empty()) {
    write_joined(full_column_list, columns_pk, print_column);
    // tiny troubleshooting assist; primary columns are separated from regular
    // columns by 2 spaces
    full_column_list << ",  ";
  }
  write_joined(full_column_list, columns_regular, print_column);

  return full_column_list.str();
}

MdSqlGenerator::MdSqlGenerator(std::shared_ptr<mdlog::MdLog> &logger_)
    : logger(logger_) {}

void MdSqlGenerator::run_query(duckdb::Connection &con,
                               const std::string &log_prefix,
                               const std::string &query,
                               const std::string &error_message) {
  logger->info(log_prefix + ": " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error(error_message + ": " + result->GetError());
  }
}

// DuckDB querying
// TODO: add test for schema or remove the logic if it's unused
bool MdSqlGenerator::schema_exists(duckdb::Connection &con,
                                   const std::string &db_name,
                                   const std::string &schema_name) {
  const std::string query =
      "SELECT schema_name FROM information_schema.schemata "
      "WHERE catalog_name=? AND schema_name=?";
  const std::string err = "Could not find whether schema <" + schema_name +
                          "> exists in database <" + db_name + ">";
  auto statement = con.Prepare(query);
  if (statement->HasError()) {
    throw std::runtime_error(err + " (at bind step): " + statement->GetError());
  }
  duckdb::vector<duckdb::Value> params = {duckdb::Value(db_name),
                                          duckdb::Value(schema_name)};
  auto result = statement->Execute(params, false);
  if (result->HasError()) {
    throw std::runtime_error(err + ": " + result->GetError());
  }
  auto materialized_result = duckdb::unique_ptr_cast<
      duckdb::QueryResult, duckdb::MaterializedQueryResult>(std::move(result));

  return materialized_result->RowCount() > 0;
}

bool MdSqlGenerator::table_exists(duckdb::Connection &con,
                                  const table_def &table) {
  const std::string query =
      "SELECT table_name FROM information_schema.tables WHERE "
      "table_catalog=? AND table_schema=? AND table_name=?";
  const std::string err =
      "Could not find whether table <" + table.to_escaped_string() + "> exists";
  auto statement = con.Prepare(query);
  if (statement->HasError()) {
    throw std::runtime_error(err + " (at bind step): " + statement->GetError());
  }
  duckdb::vector<duckdb::Value> params = {duckdb::Value(table.db_name),
                                          duckdb::Value(table.schema_name),
                                          duckdb::Value(table.table_name)};
  auto result = statement->Execute(params, false);

  if (result->HasError()) {
    throw std::runtime_error(err + ": " + result->GetError());
  }
  auto materialized_result = duckdb::unique_ptr_cast<
      duckdb::QueryResult, duckdb::MaterializedQueryResult>(std::move(result));
  return materialized_result->RowCount() > 0;
}

void MdSqlGenerator::create_schema(duckdb::Connection &con,
                                   const std::string &db_name,
                                   const std::string &schema_name) {
  auto query = "CREATE schema " + KeywordHelper::WriteQuoted(schema_name, '\'');
  logger->info("create_schema: " + query);
  con.Query(query);
}

std::string get_default_value(duckdb::LogicalTypeId type) {
  switch (type) {
  case duckdb::LogicalTypeId::VARCHAR:
    return "''";
  case duckdb::LogicalTypeId::DATE:
  case duckdb::LogicalTypeId::TIMESTAMP:
  case duckdb::LogicalTypeId::TIMESTAMP_TZ:
    return "'epoch'";
  default:
    return "0";
  }
}

void MdSqlGenerator::create_table(
    duckdb::Connection &con, const table_def &table,
    const std::vector<column_def> &all_columns,
    const std::set<std::string> &columns_with_default_value) {
  const std::string absolute_table_name = table.to_escaped_string();

  std::vector<const column_def *> columns_pk;
  find_primary_keys(all_columns, columns_pk);

  std::ostringstream ddl;
  ddl << "CREATE OR REPLACE TABLE " << absolute_table_name << " (";

  for (const auto &col : all_columns) {
    ddl << KeywordHelper::WriteQuoted(col.name, '"') << " "
        << duckdb::EnumUtil::ToChars(col.type);
    if (col.type == duckdb::LogicalTypeId::DECIMAL) {
      ddl << " (" << col.width << "," << col.scale << ")";
    }
    if (columns_with_default_value.find(col.name) !=
        columns_with_default_value.end()) {
      ddl << " DEFAULT " + get_default_value(col.type);
    }

    ddl << ", "; // DuckDB allows trailing commas
  }

  if (!columns_pk.empty()) {
    ddl << "PRIMARY KEY (";
    write_joined(ddl, columns_pk, print_column);
    ddl << ")";
  }

  ddl << ")";

  auto query = ddl.str();
  logger->info("create_table: " + query);

  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error("Could not create table <" + absolute_table_name +
                             ">" + result->GetError());
  }
}

std::vector<column_def> MdSqlGenerator::describe_table(duckdb::Connection &con,
                                                       const table_def &table) {
  // TBD is_identity is never set, used is_nullable=no temporarily but really
  // should use duckdb_constraints table.

  std::vector<column_def> columns;

  auto query = "SELECT "
               "column_name, "
               "data_type_id, "
               "NOT is_nullable, "
               "numeric_precision, "
               "numeric_scale "
               "FROM duckdb_columns() "
               "WHERE database_name=? "
               "AND schema_name=? "
               "AND table_name=?";
  const std::string err =
      "Could not describe table <" + table.to_escaped_string() + ">";
  logger->info("describe_table: " + std::string(query));
  auto statement = con.Prepare(query);
  if (statement->HasError()) {
    throw std::runtime_error(err + " (at bind step): " + statement->GetError());
  }
  duckdb::vector<duckdb::Value> params = {duckdb::Value(table.db_name),
                                          duckdb::Value(table.schema_name),
                                          duckdb::Value(table.table_name)};
  auto result = statement->Execute(params, false);

  if (result->HasError()) {
    throw std::runtime_error(err + ": " + result->GetError());
  }
  auto materialized_result = duckdb::unique_ptr_cast<
      duckdb::QueryResult, duckdb::MaterializedQueryResult>(std::move(result));

  for (const auto &row : materialized_result->Collection().GetRows()) {
    duckdb::LogicalTypeId column_type =
        static_cast<duckdb::LogicalTypeId>(row.GetValue(1).GetValue<int8_t>());
    column_def col{row.GetValue(0).GetValue<duckdb::string>(), column_type,
                   row.GetValue(2).GetValue<bool>(), 0, 0};
    if (column_type == duckdb::LogicalTypeId::DECIMAL) {
      col.width = row.GetValue(3).GetValue<uint32_t>();
      col.scale = row.GetValue(4).GetValue<uint32_t>();
    }
    columns.push_back(col);
  }
  return columns;
}

void MdSqlGenerator::alter_table_recreate(
    duckdb::Connection &con, const table_def &table,
    const std::vector<column_def> &all_columns,
    const std::set<std::string> &common_columns) {
  long timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  auto temp_table =
      table_def{table.db_name, table.schema_name,
                "tmp_" + table.table_name + "_" + std::to_string(timestamp)};
  auto absolute_table_name = table.to_escaped_string();
  auto absolute_temp_table_name = temp_table.to_escaped_string();

  run_query(con, "renaming table",
            "ALTER TABLE " + table.to_escaped_string() + " RENAME TO " +
                KeywordHelper::WriteQuoted(temp_table.table_name, '"'),
            "Could not rename table <" + absolute_table_name + ">");

  std::set<std::string> new_primary_key_cols;
  for (const auto &col : all_columns) {
    if (col.primary_key &&
        common_columns.find(col.name) == common_columns.end()) {
      new_primary_key_cols.insert(col.name);
    }
  }

  create_table(con, table, all_columns, new_primary_key_cols);

  std::ostringstream out_column_list;
  bool first = true;
  for (auto &col : common_columns) {
    if (first) {
      first = false;
    } else {
      out_column_list << ",";
    }
    out_column_list << KeywordHelper::WriteQuoted(col, '"');
  }
  std::string common_column_list = out_column_list.str();

  std::ostringstream out;
  out << "INSERT INTO " << absolute_table_name << "(" << common_column_list
      << ") SELECT " << common_column_list << " FROM "
      << absolute_temp_table_name;

  run_query(con, "Reinserting data after changing primary keys", out.str(),
            "Could not reinsert data into table <" + absolute_table_name + ">");
  run_query(con, "Dropping temp table after recreating table with constraints",
            "DROP TABLE " + absolute_temp_table_name,
            "Could not drop table <" + absolute_temp_table_name + ">");
}

void MdSqlGenerator::alter_table_in_place(
    duckdb::Connection &con, const std::string &absolute_table_name,
    const std::set<std::string> &added_columns,
    const std::set<std::string> &deleted_columns,
    const std::set<std::string> &alter_types,
    const std::map<std::string, column_def> &new_column_map) {
  for (const auto &col_name : added_columns) {
    std::ostringstream out;
    out << "ALTER TABLE " << absolute_table_name << " ADD COLUMN ";
    const auto &col = new_column_map.at(col_name);

    out << KeywordHelper::WriteQuoted(col_name, '"') << " "
        << duckdb::EnumUtil::ToChars(col.type);

    run_query(con, "alter_table add", out.str(),
              "Could not add column <" + col_name + "> to table <" +
                  absolute_table_name + ">");
  }

  for (const auto &col_name : deleted_columns) {
    std::ostringstream out;
    out << "ALTER TABLE " << absolute_table_name << " DROP COLUMN ";

    out << KeywordHelper::WriteQuoted(col_name, '"');

    run_query(con, "alter_table drop", out.str(),
              "Could not drop column <" + col_name + "> from table <" +
                  absolute_table_name + ">");
  }

  for (const auto &col_name : alter_types) {
    std::ostringstream out;
    out << "ALTER TABLE " << absolute_table_name << " ALTER ";
    const auto &col = new_column_map.at(col_name);

    out << KeywordHelper::WriteQuoted(col_name, '"') << " TYPE "
        << duckdb::EnumUtil::ToChars(col.type);

    run_query(con, "alter table change type", out.str(),
              "Could not alter type for column <" + col_name + "> in table <" +
                  absolute_table_name + ">");
  }
}

void MdSqlGenerator::alter_table(duckdb::Connection &con,
                                 const table_def &table,
                                 const std::vector<column_def> &columns) {

  bool recreate_table = false;

  auto absolute_table_name = table.to_escaped_string();
  std::set<std::string> alter_types;
  std::set<std::string> added_columns;
  std::set<std::string> deleted_columns;
  std::set<std::string> common_columns;

  const auto &existing_columns = describe_table(con, table);
  std::map<std::string, column_def> new_column_map;

  for (const auto &col : columns) {
    new_column_map.emplace(col.name, col);
    added_columns.emplace(col.name);
  }

  for (const auto &col : existing_columns) {
    const auto &new_col_it = new_column_map.find(col.name);

    if (added_columns.erase(col.name)) {
      common_columns.emplace(col.name);
    }

    if (new_col_it == new_column_map.end()) {
      deleted_columns.emplace(col.name);
      if (col.primary_key) {
        recreate_table = true;
      }
    } else if (new_col_it->second.primary_key != col.primary_key) {
      logger->info("Altering primary key requested for column <" +
                   new_col_it->second.name + ">");
      recreate_table = true;
    } else if (new_col_it->second.type != col.type) {
      alter_types.emplace(col.name);
    }
  }

  auto primary_key_added_it =
      std::find_if(added_columns.begin(), added_columns.end(),
                   [&new_column_map](const std::string &column_name) {
                     return new_column_map[column_name].primary_key;
                   });
  if (primary_key_added_it != added_columns.end()) {
    logger->info("Adding primary key requested for column <" +
                 *primary_key_added_it + ">");
    recreate_table = true;
  }

  run_query(con, "begin alter table transaction", "BEGIN TRANSACTION",
            "Could not begin transaction for altering table <" +
                absolute_table_name + ">");

  if (recreate_table) {
    alter_table_recreate(con, table, columns, common_columns);
  } else {
    alter_table_in_place(con, absolute_table_name, added_columns,
                         deleted_columns, alter_types, new_column_map);
  }

  run_query(con, "commit alter table transaction", "END TRANSACTION",
            "Could not commit transaction for altering table <" +
                absolute_table_name + ">");
}

void MdSqlGenerator::upsert(
    duckdb::Connection &con, const table_def &table,
    const std::string &staging_table_name,
    const std::vector<const column_def *> &columns_pk,
    const std::vector<const column_def *> &columns_regular) {

  auto full_column_list = make_full_column_list(columns_pk, columns_regular);
  const std::string absolute_table_name = table.to_escaped_string();
  std::ostringstream sql;
  sql << "INSERT INTO " << absolute_table_name << "(" << full_column_list
      << ") SELECT " << full_column_list << " FROM " << staging_table_name;
  if (!columns_pk.empty()) {
    sql << " ON CONFLICT (";
    write_joined(sql, columns_pk, print_column);
    sql << " ) DO UPDATE SET ";

    write_joined(sql, columns_regular,
                 [](const std::string &quoted_col, std::ostringstream &out) {
                   out << quoted_col << " = excluded." << quoted_col;
                 });
  }

  auto query = sql.str();
  logger->info("upsert: " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error("Could not upsert table <" + absolute_table_name +
                             ">" + result->GetError());
  }
}

void MdSqlGenerator::update_values(
    duckdb::Connection &con, const table_def &table,
    const std::string &staging_table_name,
    std::vector<const column_def *> &columns_pk,
    std::vector<const column_def *> &columns_regular,
    const std::string &unmodified_string) {

  std::ostringstream sql;
  auto absolute_table_name = table.to_escaped_string();

  sql << "UPDATE " << absolute_table_name << " SET ";

  write_joined(sql, columns_regular,
               [staging_table_name, absolute_table_name, unmodified_string](
                   const std::string quoted_col, std::ostringstream &out) {
                 out << quoted_col << " = CASE WHEN " << staging_table_name
                     << "." << quoted_col << " = "
                     << KeywordHelper::WriteQuoted(unmodified_string, '\'')
                     << " THEN " << absolute_table_name << "." << quoted_col
                     << " ELSE " << staging_table_name << "." << quoted_col
                     << " END";
               });

  sql << " FROM " << staging_table_name << " WHERE ";
  write_joined(
      sql, columns_pk,
      [&](const std::string &quoted_col, std::ostringstream &out) {
        out << KeywordHelper::WriteQuoted(table.table_name, '"') << "."
            << quoted_col << " = " << staging_table_name << "." << quoted_col;
      },
      " AND ");

  auto query = sql.str();
  logger->info("update: " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error("Could not update table <" + absolute_table_name +
                             ">:" + result->GetError());
  }
}

void MdSqlGenerator::delete_rows(duckdb::Connection &con,
                                 const table_def &table,
                                 const std::string &staging_table_name,
                                 std::vector<const column_def *> &columns_pk) {

  const std::string absolute_table_name = table.to_escaped_string();
  std::ostringstream sql;
  sql << "DELETE FROM " + absolute_table_name << " USING " << staging_table_name
      << " WHERE ";

  write_joined(
      sql, columns_pk,
      [&](const std::string &quoted_col, std::ostringstream &out) {
        out << KeywordHelper::WriteQuoted(table.table_name, '"') << "."
            << quoted_col << " = " << staging_table_name << "." << quoted_col;
      },
      " AND ");

  auto query = sql.str();
  logger->info("delete_rows: " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error("Error deleting rows from table <" +
                             absolute_table_name + ">:" + result->GetError());
  }
}

void MdSqlGenerator::truncate_table(duckdb::Connection &con,
                                    const table_def &table,
                                    const std::string &synced_column,
                                    std::chrono::nanoseconds &cutoff_ns,
                                    const std::string &deleted_column) {
  const std::string absolute_table_name = table.to_escaped_string();
  std::ostringstream sql;

  logger->info("truncate_table request: deleted column = " + deleted_column);
  if (deleted_column.empty()) {
    // hard delete
    sql << "DELETE FROM " << absolute_table_name;
  } else {
    // soft delete
    sql << "UPDATE " << absolute_table_name << " SET "
        << KeywordHelper::WriteQuoted(deleted_column, '"') << " = true";
  }
  logger->info("truncate_table request: synced column = " + synced_column);
  sql << " WHERE " << KeywordHelper::WriteQuoted(synced_column, '"')
      << " < make_timestamp(?)";
  auto query = sql.str();
  const std::string err =
      "Error truncating table at bind step <" + absolute_table_name + ">";
  logger->info("truncate_table: " + query);
  auto statement = con.Prepare(query);
  if (statement->HasError()) {
    throw std::runtime_error(err + " (at bind step):" + statement->GetError());
  }

  // DuckDB make_timestamp takes microseconds; Fivetran sends millisecond
  // precision -- safe to divide with truncation
  int64_t cutoff_microseconds = cutoff_ns.count() / 1000;
  duckdb::vector<duckdb::Value> params = {duckdb::Value(cutoff_microseconds)};

  logger->info("truncate_table: cutoff_microseconds = <" +
               std::to_string(cutoff_microseconds) + ">");
  auto result = statement->Execute(params, false);
  if (result->HasError()) {
    throw std::runtime_error(err + ": " + result->GetError());
  }
}

void MdSqlGenerator::check_connection(duckdb::Connection &con) {
  auto result = con.Query("PRAGMA MD_VERSION");
  if (result->HasError()) {
    throw std::runtime_error("Error checking connection: " +
                             result->GetError());
  }
}
