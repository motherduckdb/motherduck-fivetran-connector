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
    std::function<void(const std::string &, std::ostringstream &)> print_str) {
  bool first = true;
  for (const auto &col : columns) {
    if (first) {
      first = false;
    } else {
      sql << ", ";
    }
    print_str(KeywordHelper::WriteQuoted(col->name, '"'), sql);
  }
}

MdSqlGenerator::MdSqlGenerator(std::shared_ptr<mdlog::MdLog> &logger_): logger(logger_) {}

// DuckDB querying
// TODO: add test for schema or remove the logic if it's unused
bool MdSqlGenerator::schema_exists(duckdb::Connection &con, const std::string &db_name,
                   const std::string &schema_name) {
  const std::string query = "SELECT schema_name FROM information_schema.schemata "
                      "WHERE catalog_name=? AND schema_name=?";
  auto statement = con.Prepare(query);
  duckdb::vector<duckdb::Value> params = {duckdb::Value(db_name),
                                          duckdb::Value(schema_name)};
  auto result = statement->Execute(params, false);
  if (result->HasError()) {
    throw std::runtime_error("Could not find whether schema <" + schema_name +
                             "> exists in database <" + db_name +
                             ">: " + result->GetError());
  }
  auto materialized_result = duckdb::unique_ptr_cast<
      duckdb::QueryResult, duckdb::MaterializedQueryResult>(std::move(result));

  return materialized_result->RowCount() > 0;
}

bool MdSqlGenerator::table_exists(duckdb::Connection &con, const table_def &table) {
  const std::string query = "SELECT table_name FROM information_schema.tables WHERE "
                      "table_catalog=? AND table_schema=? AND table_name=?";
  auto statement = con.Prepare(query);
  duckdb::vector<duckdb::Value> params = {duckdb::Value(table.db_name),
                                          duckdb::Value(table.schema_name),
                                          duckdb::Value(table.table_name)};
  auto result = statement->Execute(params, false);

  if (result->HasError()) {
    throw std::runtime_error("Could not find whether table <" +
                             table.to_escaped_string() +
                             "> exists: " + result->GetError());
  }
  auto materialized_result = duckdb::unique_ptr_cast<
      duckdb::QueryResult, duckdb::MaterializedQueryResult>(std::move(result));
  return materialized_result->RowCount() > 0;
}

void MdSqlGenerator::create_schema(duckdb::Connection &con, const std::string &db_name,
                   const std::string &schema_name) {
  auto query = "CREATE schema " + KeywordHelper::WriteQuoted(schema_name, '\'');
  logger->info("create_schema: " + query);
  con.Query(query);
}

void MdSqlGenerator::create_table(duckdb::Connection &con, const table_def &table,
                  const std::vector<const column_def *> &columns_pk,
                  const std::vector<column_def> &all_columns) {
  const std::string absolute_table_name = table.to_escaped_string();
  std::ostringstream ddl;
  ddl << "CREATE OR REPLACE TABLE " << absolute_table_name << " (";

  for (const auto &col : all_columns) {
    ddl << KeywordHelper::WriteQuoted(col.name, '"') << " "
        << duckdb::EnumUtil::ToChars(col.type);
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

  // TBD scale/precision
  std::vector<column_def> columns;

  auto query = "SELECT column_name, data_type, is_nullable = 'NO' FROM "
               "information_schema.columns WHERE table_catalog=? AND "
               "table_schema=? AND table_name=?";
  logger->info("describe_table: " + std::string(query));
  auto statement = con.Prepare(query);
  duckdb::vector<duckdb::Value> params = {duckdb::Value(table.db_name),
                                          duckdb::Value(table.schema_name),
                                          duckdb::Value(table.table_name)};
  auto result = statement->Execute(params, false);

  if (result->HasError()) {
    throw std::runtime_error("Could not describe table <" +
                             table.to_escaped_string() +
                             ">:" + result->GetError());
  }
  auto materialized_result = duckdb::unique_ptr_cast<
      duckdb::QueryResult, duckdb::MaterializedQueryResult>(std::move(result));

  for (const auto &row : materialized_result->Collection().GetRows()) {
    columns.push_back(
        column_def{row.GetValue(0).GetValue<duckdb::string>(),
                   duckdb::EnumUtil::FromString<duckdb::LogicalTypeId>(
                       row.GetValue(1).GetValue<duckdb::string>()),
                   row.GetValue(2).GetValue<bool>()});
  }
  return columns;
}

void MdSqlGenerator::alter_table(duckdb::Connection &con, const table_def &table,
                 const std::vector<column_def> &columns) {

  auto absolute_table_name = table.to_escaped_string();
  std::set<std::string> alter_types;
  std::set<std::string> added_columns;
  std::set<std::string> deleted_columns;

  const auto &existing_columns = describe_table(con, table);
  std::map<std::string, column_def> new_column_map;

  for (const auto &col : columns) {
    new_column_map.emplace(col.name, col);
    added_columns.emplace(col.name);
  }

  for (const auto &col : existing_columns) {
    const auto &new_col_it = new_column_map.find(col.name);

    added_columns.erase(col.name);

    if (new_col_it == new_column_map.end()) {
      deleted_columns.emplace(col.name);
    } else if (new_col_it->second.type !=
               col.type) { // altering primary key not supported in duckdb
      alter_types.emplace(col.name);
    }
  }

  for (const auto &col_name : added_columns) {
    std::ostringstream out;
    out << "ALTER TABLE " << absolute_table_name << " ADD COLUMN ";
    const auto &col = new_column_map[col_name];

    out << KeywordHelper::WriteQuoted(col_name, '"') << " "
        << duckdb::EnumUtil::ToChars(col.type);
    if (col.primary_key) {
      out << " PRIMARY KEY";
    }
    auto query = out.str();
    logger->info("alter_table: " + query);
    auto result = con.Query(query);
    if (result->HasError()) {
      throw std::runtime_error("Could not add column <" + col_name +
                               "> to table <" + absolute_table_name +
                               ">:" + result->GetError());
    }
  }

  for (const auto &col_name : deleted_columns) {
    std::ostringstream out;
    out << "ALTER TABLE " << absolute_table_name << " DROP COLUMN ";

    out << KeywordHelper::WriteQuoted(col_name, '"');

    auto query = out.str();
    logger->info("alter_table: " + query);
    auto result = con.Query(query);
    if (result->HasError()) {
      throw std::runtime_error("Could not drop column <" + col_name +
                               "> from table <" + absolute_table_name +
                               ">:" + result->GetError());
    }
  }

  for (const auto &col_name : alter_types) {
    std::ostringstream out;
    out << "ALTER TABLE " << absolute_table_name << " ALTER ";
    const auto &col = new_column_map[col_name];

    out << KeywordHelper::WriteQuoted(col_name, '"') << " TYPE "
        << duckdb::EnumUtil::ToChars(col.type);

    auto query = out.str();
    logger->info("alter table: " + query);
    auto result = con.Query(query);
    if (result->HasError()) {
      throw std::runtime_error("Could not alter type for column <" + col_name +
                               "> in table <" + absolute_table_name +
                               ">:" + result->GetError());
    }
  }
}

std::string
make_full_column_list(const std::vector<const column_def *> &columns_pk,
                      const std::vector<const column_def *> &columns_regular) {
  std::ostringstream full_column_list;
  if (!columns_pk.empty()) {
    write_joined(full_column_list, columns_pk, print_column);
    // tiny troubleshooting assist; primary columns are separated from regular columns by 2 spaces
    full_column_list << ",  ";
  }
  write_joined(full_column_list, columns_regular, print_column);

  return full_column_list.str();
}

void MdSqlGenerator::upsert(duckdb::Connection &con, const table_def &table,
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

void MdSqlGenerator::update_values(duckdb::Connection &con, const table_def &table,
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
  write_joined(sql, columns_pk,
               [&](const std::string &quoted_col, std::ostringstream &out) {
                 out << table.table_name << "." << quoted_col << " = "
                     << staging_table_name << "." << quoted_col;
               });

  auto query = sql.str();
  logger->info("update: " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error("Could not update table <" + absolute_table_name +
                             ">:" + result->GetError());
  }
}

void MdSqlGenerator::delete_rows(duckdb::Connection &con, const table_def &table,
                 const std::string &staging_table_name,
                 std::vector<const column_def *> &columns_pk) {

  const std::string absolute_table_name = table.to_escaped_string();
  std::ostringstream sql;
  sql << "DELETE FROM " + absolute_table_name << " USING " << staging_table_name
      << " WHERE ";

  write_joined(sql, columns_pk,
               [&](const std::string &quoted_col, std::ostringstream &out) {
                 out << table.table_name << "." << quoted_col << " = "
                     << staging_table_name << "." << quoted_col;
               });

  auto query = sql.str();
  logger->info("delete_rows: " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error("Error deleting rows from table <" +
                             absolute_table_name + ">:" + result->GetError());
  }
}

void MdSqlGenerator::truncate_table(duckdb::Connection &con, const table_def &table,
                    const std::string &synced_column,
                    std::chrono::nanoseconds &cutoff_ns,
                    const std::string &deleted_column) {
  const std::string absolute_table_name = table.to_escaped_string();
  std::ostringstream sql;

  if (deleted_column.empty()) {
    // hard delete
    sql << "DELETE FROM " << absolute_table_name;
  } else {
    // soft delete
    sql << "UPDATE " << absolute_table_name << " SET "
        << KeywordHelper::WriteQuoted(deleted_column, '"') << " = true";
  }
  sql << " WHERE " << KeywordHelper::WriteQuoted(synced_column, '"')
      << " < make_timestamp(?)";
  auto query = sql.str();
  logger->info("truncate_table: " + query);
  auto statement = con.Prepare(query);

  // DuckDB make_timestamp takes microseconds; Fivetran sends millisecond
  // precision -- safe to divide with truncation
  int64_t cutoff_microseconds = cutoff_ns.count() / 1000;
  duckdb::vector<duckdb::Value> params = {duckdb::Value(cutoff_microseconds)};

  logger->info("truncate_table: cutoff_microseconds = <" + std::to_string(cutoff_microseconds) + ">");
  auto result = statement->Execute(params, false);
  if (result->HasError()) {
    throw std::runtime_error("Error truncating table <" + absolute_table_name +
                             ">:" + result->GetError());
  }
}

void MdSqlGenerator::check_connection(duckdb::Connection &con) {
  auto result = con.Query("PRAGMA MD_VERSION");
  if (result->HasError()) {
    throw std::runtime_error("Error checking connection: " +
                             result->GetError());
  }
}
