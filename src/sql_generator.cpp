#include <iostream>
#include <md_logging.hpp>
#include <string>

#include "fivetran_duckdb_interop.hpp"
#include "sql_generator.hpp"

#include "md_error.hpp"

using duckdb::KeywordHelper;

// Utility

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
                       std::vector<const column_def *> *columns_regular,
                       const std::string &ignored_primary_key) {
  for (auto &col : cols) {
    if (col.primary_key && col.name != ignored_primary_key) {
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

const std::string primary_key_join(std::vector<const column_def *> &columns_pk,
                                   const std::string tbl1,
                                   const std::string tbl2) {
  std::ostringstream primary_key_join_condition_stream;
  write_joined(
      primary_key_join_condition_stream, columns_pk,
      [&](const std::string &quoted_col, std::ostringstream &out) {
        out << tbl1 << "." << quoted_col << " = " << tbl2 << "." << quoted_col;
      },
      " AND ");
  return primary_key_join_condition_stream.str();
}

/*
 * create an empty table structure in-memory to store latest active records.
 * this will not be cleaned up manually just in case further update files need
 * to refer to the latest values.
 */
std::string
create_latest_active_records_table(duckdb::Connection &con,
                                   const std::string &absolute_table_name,
                                   const std::string &temp_db_name) {
  const std::string table_name =
      "\"" + temp_db_name + R"("."main"."latest_active_records")";
  const auto create_res =
      con.Query("CREATE TABLE IF NOT EXISTS " + table_name + " AS FROM " +
                absolute_table_name + " WITH NO DATA");
  if (create_res->HasError()) {
    create_res->ThrowError("Could not create latest_active_records table: ");
  }
  return table_name;
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
  logger->info("    prepared table_exists query for table " + table.table_name);
  if (statement->HasError()) {
    throw std::runtime_error(err + " (at bind step): " + statement->GetError());
  }
  duckdb::vector<duckdb::Value> params = {duckdb::Value(table.db_name),
                                          duckdb::Value(table.schema_name),
                                          duckdb::Value(table.table_name)};
  auto result = statement->Execute(params, false);
  logger->info("    executed table_exists query for table " + table.table_name);

  if (result->HasError()) {
    throw std::runtime_error(err + ": " + result->GetError());
  }
  auto materialized_result = duckdb::unique_ptr_cast<
      duckdb::QueryResult, duckdb::MaterializedQueryResult>(std::move(result));
  logger->info("    materialized table_exists results for table " +
               table.table_name);
  return materialized_result->RowCount() > 0;
}

void MdSqlGenerator::create_schema(duckdb::Connection &con,
                                   const std::string &db_name,
                                   const std::string &schema_name) {
  std::ostringstream ddl;
  ddl << "CREATE SCHEMA " << KeywordHelper::WriteQuoted(db_name, '"') << "."
      << KeywordHelper::WriteQuoted(schema_name, '"');
  const std::string query = ddl.str();

  logger->info("create_schema: " + query);
  const auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error("Could not create schema <" + schema_name +
                             "> in database <" + db_name +
                             ">: " + result->GetError());
  }
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
    ddl << KeywordHelper::WriteQuoted(col.name, '"') << " " << format_type(col);
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

  const auto query = ddl.str();
  logger->info("create_table: " + query);

  const auto result = con.Query(query);
  if (result->HasError()) {
    const std::string &error_msg = result->GetError();

    if (error_msg.find("is attached in read-only mode") != std::string::npos) {
      throw md_error::RecoverableError(
          "The database is attached in read-only mode. Please make sure your "
          "MotherDuck token is a Read/Write "
          "Token and check that it can write to the target database.");
    }

    throw std::runtime_error("Could not create table <" + absolute_table_name +
                             ">: " + error_msg);
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
               "column_default, "
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

  auto &materialized_result = result->Cast<duckdb::MaterializedQueryResult>();

  for (const auto &row : materialized_result.Collection().GetRows()) {
    duckdb::LogicalTypeId column_type =
        static_cast<duckdb::LogicalTypeId>(row.GetValue(1).GetValue<int8_t>());
    column_def col{row.GetValue(0).GetValue<duckdb::string>(),
                   column_type,
                   row.GetValue(2).GetValue<duckdb::string>(),
                   row.GetValue(3).GetValue<bool>(),
                   0,
                   0};
    if (column_type == duckdb::LogicalTypeId::DECIMAL) {
      col.width = row.GetValue(4).GetValue<uint32_t>();
      col.scale = row.GetValue(5).GetValue<uint32_t>();
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

  // new primary keys have to get a default value as they cannot be null
  std::set<std::string> new_primary_key_cols;
  for (const auto &col : all_columns) {
    if (col.primary_key &&
        common_columns.find(col.name) == common_columns.end()) {
      new_primary_key_cols.insert(col.name);
    }
  }

  create_table(con, table, all_columns, new_primary_key_cols);

  // reinsert the data from the old table
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
    const std::vector<column_def> &added_columns,
    const std::set<std::string> &deleted_columns,
    const std::set<std::string> &alter_types,
    const std::map<std::string, column_def> &new_column_map) {
  for (const auto &col : added_columns) {
    std::ostringstream out;
    out << "ALTER TABLE " << absolute_table_name << " ADD COLUMN ";

    out << KeywordHelper::WriteQuoted(col.name, '"') << " "
        << duckdb::EnumUtil::ToChars(col.type);

    run_query(con, "alter_table add", out.str(),
              "Could not add column <" + col.name + "> to table <" +
                  absolute_table_name + ">");
  }

  for (const auto &col_name : alter_types) {
    std::ostringstream out;
    out << "ALTER TABLE " << absolute_table_name << " ALTER ";
    const auto &col = new_column_map.at(col_name);

    out << KeywordHelper::WriteQuoted(col_name, '"') << " TYPE "
        << format_type(col);

    run_query(con, "alter table change type", out.str(),
              "Could not alter type for column <" + col_name + "> in table <" +
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
}

void MdSqlGenerator::alter_table(
    duckdb::Connection &con, const table_def &table,
    const std::vector<column_def> &requested_columns, const bool drop_columns) {
  bool recreate_table = false;

  auto absolute_table_name = table.to_escaped_string();
  std::set<std::string> alter_types;
  std::set<std::string> added_columns;
  std::set<std::string> deleted_columns;
  std::set<std::string> common_columns;

  logger->info("    in MdSqlGenerator::alter_table for " + absolute_table_name);
  const auto &existing_columns = describe_table(con, table);
  std::map<std::string, column_def> new_column_map;

  // start by assuming all columns are new
  for (const auto &col : requested_columns) {
    new_column_map.emplace(col.name, col);
    added_columns.emplace(col.name);
  }

  // make added_columns correct by removing previously existing columns
  for (const auto &col : existing_columns) {
    const auto &new_col_it = new_column_map.find(col.name);

    if (added_columns.erase(col.name)) {
      common_columns.emplace(col.name);
    }

    if (new_col_it == new_column_map.end()) {
      if (drop_columns) { // Only drop physical columns if drop_columns is true
                          // (from the alter table request)
        deleted_columns.emplace(col.name);

        if (col.primary_key) {
          recreate_table = true;
        }
      } else {
        logger->info("Source connector requested that table " +
                     absolute_table_name + " column " + col.name +
                     " be dropped, but dropping columns is not allowed when "
                     "drop_columns is false");
      }
    } else if (new_col_it->second.primary_key != col.primary_key) {
      logger->info("Altering primary key requested for column <" +
                   new_col_it->second.name + ">");
      recreate_table = true;
    } else if (new_col_it->second.type != col.type ||
               new_col_it->second.type == duckdb::LogicalTypeId::DECIMAL &&
                   (new_col_it->second.scale != col.scale ||
                    new_col_it->second.width != col.width)) {
      alter_types.emplace(col.name);
    }
  }
  logger->info("    inventoried columns; recreate_table = " +
               std::to_string(recreate_table) +
               "; num alter_types = " + std::to_string(alter_types.size()));

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

  // list added columns in order
  std::vector<column_def> added_columns_ordered;
  for (const auto &col : requested_columns) {
    const auto &new_col_it = added_columns.find(col.name);
    if (new_col_it != added_columns.end()) {
      added_columns_ordered.push_back(new_column_map[col.name]);
      logger->info("    adding column " + col.name);
    }
  }

  run_query(con, "begin alter table transaction", "BEGIN TRANSACTION",
            "Could not begin transaction for altering table <" +
                absolute_table_name + ">");

  if (recreate_table) {
    logger->info("    recreating table");
    // preserve the order of the original columns
    auto all_columns = existing_columns;

    // replace definitions of existing columns with the new ones if available
    for (size_t i = 0; i < all_columns.size(); i++) {
      const auto &new_col_it = new_column_map.find(all_columns[i].name);
      if (new_col_it != new_column_map.end()) {
        all_columns[i] = new_col_it->second;
      }
    }

    // add new columns to the end of the table, in order they appear in the
    // request
    for (const auto &col : added_columns_ordered) {
      all_columns.push_back(col);
    }
    alter_table_recreate(con, table, all_columns, common_columns);
  } else {
    logger->info("    altering table in place");
    alter_table_in_place(con, absolute_table_name, added_columns_ordered,
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

void MdSqlGenerator::insert(
    duckdb::Connection &con, const table_def &table,
    const std::string &staging_table_name,
    const std::vector<const column_def *> &columns_pk,
    const std::vector<const column_def *> &columns_regular) {

  auto full_column_list = make_full_column_list(columns_pk, columns_regular);
  const std::string absolute_table_name = table.to_escaped_string();
  std::ostringstream sql;
  sql << "INSERT INTO " << absolute_table_name << "(" << full_column_list
      << ") SELECT " << full_column_list << " FROM " << staging_table_name;

  auto query = sql.str();
  logger->info("insert: " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error("Could not insert into table <" +
                             absolute_table_name + ">" + result->GetError());
  }
}

void MdSqlGenerator::update_values(
    duckdb::Connection &con, const table_def &table,
    const std::string &staging_table_name,
    std::vector<const column_def *> &columns_pk,
    std::vector<const column_def *> &columns_regular,
    const std::string &unmodified_string) {

  logger->info("MdSqlGenerator::update_values requested");
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

// TODO: Remove unused arguments
void MdSqlGenerator::add_partial_historical_values(
    duckdb::Connection &con, const table_def &table,
    const std::string &staging_table_name,
    std::vector<const column_def *> &columns_pk,
    std::vector<const column_def *> &columns_regular,
    const std::string &unmodified_string, const std::string &temp_db_name) {
  std::ostringstream sql;
  auto absolute_table_name = table.to_escaped_string();

  /*
  The latest_active_records (lar) table is used to process the update file
  from fivetran in history mode. We receive a file in which only updated
  columns are provided, so we need to "manually" fetch the values for the
  remaining columns to be able to insert a new valid row with all the right
  columns values. As this uses type 2 slowly changing dimensions, i.e. insert
  a new row on updates, we cannot use UPDATE x SET y = value, as this updates
  in place.
  */

  // create empty table structure just in case there were not any earliest files
  // that would have created it.
  const auto lar_table = create_latest_active_records_table(
      con, absolute_table_name, temp_db_name);

  auto full_column_list = make_full_column_list(columns_pk, columns_regular);
  sql << "INSERT INTO " << absolute_table_name << " (" << full_column_list
      << ") ( SELECT ";

  // use primary keys as is, without checking for unmodified value
  write_joined(
      sql, columns_pk,
      [&](const std::string &quoted_col, std::ostringstream &out) {
        out << staging_table_name << "." << quoted_col;
      },
      ", ");
  sql << ",  ";

  write_joined(
      sql, columns_regular,
      [staging_table_name, absolute_table_name, unmodified_string,
       lar_table](const std::string quoted_col, std::ostringstream &out) {
        out << "CASE WHEN " << staging_table_name << "." << quoted_col << " = "
            << KeywordHelper::WriteQuoted(unmodified_string, '\'')
            << " THEN lar." << quoted_col << " ELSE " << staging_table_name
            << "." << quoted_col << " END as " << quoted_col;
      });

  sql << " FROM " << staging_table_name << " LEFT JOIN " << lar_table
      << " AS lar ON "
      << primary_key_join(columns_pk, "lar", staging_table_name) << ")";

  auto query = sql.str();
  logger->info("update (add partial historical values): " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error(
        "Could not update (add partial historical values) table <" +
        absolute_table_name + ">:" + result->GetError());
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

void MdSqlGenerator::deactivate_historical_records(
    duckdb::Connection &con, const table_def &table,
    const std::string &staging_table_name,
    std::vector<const column_def *> &columns_pk,
    const std::string &temp_db_name) {

  const std::string absolute_table_name = table.to_escaped_string();

  // persist the data (it's only 2 columns) to not read CSV file twice
  const std::string temp_earliest_table_name = "local_earliest_file";
  con.Query("CREATE TABLE " + temp_earliest_table_name + " AS SELECT * FROM " +
            staging_table_name);

  // primary keys condition (list of primary keys already excludes
  // _fivetran_start)
  std::string primary_key_join_condition = primary_key_join(
      columns_pk, absolute_table_name, temp_earliest_table_name);

  {
    // delete overlapping records
    std::ostringstream sql;
    sql << "DELETE FROM " + absolute_table_name << " USING "
        << temp_earliest_table_name << " WHERE ";
    sql << primary_key_join_condition;
    sql << " AND " << absolute_table_name
        << "._fivetran_start >= " << temp_earliest_table_name
        << "._fivetran_start";

    auto query = sql.str();
    logger->info("delete_overlapping_records: " + query);
    auto result = con.Query(query);
    if (result->HasError()) {
      throw std::runtime_error(
          "Error deleting overlapping records from table <" +
          absolute_table_name + ">:" + result->GetError());
    }
  }

  const auto lar_table = create_latest_active_records_table(
      con, absolute_table_name, temp_db_name);

  {
    // store latest versions of records before they get deactivated
    // per spec, this should be limited to _fivetran_active = TRUE, but it's
    // safer to get all latest versions even if deactivated to prevent null
    // values in a partially successful batch.
    std::ostringstream sql;
    const std::string short_table_name =
        KeywordHelper::WriteQuoted(table.table_name, '"');
    sql << "WITH ranked_records AS (SELECT " << short_table_name << ".*,";
    sql << " row_number() OVER (PARTITION BY ";
    write_joined(sql, columns_pk,
                 [&](const std::string &quoted_col, std::ostringstream &out) {
                   out << absolute_table_name << "." << quoted_col;
                 });
    sql << " ORDER BY " << absolute_table_name
        << "._fivetran_start DESC) as row_num FROM " << absolute_table_name;
    // inner join to earliest table to only select rows that are in this batch
    sql << " INNER JOIN " << temp_earliest_table_name << " ON "
        << primary_key_join_condition << ")\n";

    sql << "INSERT INTO " << lar_table
        << " SELECT * EXCLUDE (row_num) FROM "
           "ranked_records WHERE row_num = 1";
    auto query = sql.str();
    logger->info("stash latest records: " + query);
    auto result = con.Query(query);
    if (result->HasError()) {
      throw std::runtime_error("Error stashing latest records from table <" +
                               absolute_table_name + ">:" + result->GetError());
    }
  }

  {
    // mark existing records inactive
    std::ostringstream sql;
    sql << "UPDATE " + absolute_table_name << " SET _fivetran_active = FALSE, ";
    // converting to TIMESTAMP with no timezone because otherwise ICU is
    // required to do TIMESTAMPZ math. Need to test this well.
    sql << "_fivetran_end = (" << temp_earliest_table_name
        << "._fivetran_start::TIMESTAMP - (INTERVAL '1 millisecond'))";
    sql << " FROM " << temp_earliest_table_name;
    sql << " WHERE " << absolute_table_name << "._fivetran_active = TRUE AND ";
    sql << primary_key_join_condition;

    auto query = sql.str();
    logger->info("deactivate records: " + query);
    auto result = con.Query(query);
    if (result->HasError()) {
      throw std::runtime_error("Error deactivating records <" +
                               absolute_table_name + ">:" + result->GetError());
    }
  }

  {
    // clean up the temp in memory table, so it can get recreated by another
    // earliest file
    con.Query("DROP TABLE " + temp_earliest_table_name);
  }
}

void MdSqlGenerator::delete_historical_rows(
    duckdb::Connection &con, const table_def &table,
    const std::string &staging_table_name,
    std::vector<const column_def *> &columns_pk) {

  const std::string absolute_table_name = table.to_escaped_string();

  std::string primary_key_join_condition =
      primary_key_join(columns_pk, absolute_table_name, staging_table_name);

  std::ostringstream sql;

  sql << "UPDATE " + absolute_table_name << " SET _fivetran_active = FALSE, ";
  sql << "_fivetran_end = " << staging_table_name << "._fivetran_end";
  sql << " FROM " << staging_table_name;
  sql << " WHERE " << absolute_table_name << "._fivetran_active = TRUE AND ";
  sql << primary_key_join_condition;

  auto query = sql.str();
  logger->info("delete historical records: " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error("Error deleting historical records <" +
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

// Migration operations

void MdSqlGenerator::drop_table(duckdb::Connection &con,
                                const table_def &table) {
  const std::string absolute_table_name = table.to_escaped_string();
  run_query(con, "drop_table", "DROP TABLE " + absolute_table_name,
            "Could not drop table <" + absolute_table_name + ">");
}

void MdSqlGenerator::drop_column_in_history_mode(
    duckdb::Connection &con, const table_def &table, const std::string &column,
    const std::string &operation_timestamp) {
  const std::string absolute_table_name = table.to_escaped_string();
  const std::string quoted_column = KeywordHelper::WriteQuoted(column, '"');
  const std::string quoted_timestamp =
      "'" + operation_timestamp + "'::TIMESTAMPTZ";

  if (validate_history_table(con, absolute_table_name, quoted_timestamp)) {
    return;
  }

  // Per spec: In history mode, dropping a column preserves historical data.
  // We execute 3 queries as described in the spec if the table is not empty.

  con.BeginTransaction();
  {
    // Query 1: Insert new rows for active records where column is not null
    std::ostringstream sql;
    sql << "INSERT INTO " << absolute_table_name << " SELECT * REPLACE"
        << " (NULL as" << quoted_column << ", " << quoted_timestamp
        << " as \"_fivetran_start\""
        << ")"
        << " FROM " << absolute_table_name
        << " WHERE \"_fivetran_active\" = TRUE"
        << " AND " << quoted_column << " IS NOT NULL"
        << " AND \"_fivetran_start\" < " << quoted_timestamp;

    run_query(con, "drop_column_in_history_mode insert", sql.str(),
              "Could not insert new rows for drop_column_in_history_mode");
  }

  {
    // Query 2: Update newly added rows to set column to NULL. Per the docs:
    //   "This step is important in case of source connector sends multiple
    //   DROP_COLUMN_IN_HISTORY_MODE operations with the same
    //   operation_timestamp. It will ensure, we only record history once for
    //   that timestamp."
    // To elaborate: if columns A and B are dropped at the same time in the
    // source, and we first have to drop A (as this endpoint only handles 1
    // column at a time), this operation only sets A to NULL for the
    // operation_timestamp, not B. When we receive the request to drop B, we
    // skip all the rows we already re-inserted in query 1 because of the
    // \"_fivetran_start\" < quoted_timestamp clause. Query 2 assures we also
    // set B to NULL for the rows inserted while handling column A.

    std::ostringstream sql;
    sql << "UPDATE " << absolute_table_name << " SET " << quoted_column
        << " = NULL WHERE \"_fivetran_start\" = " << quoted_timestamp;

    run_query(con, "drop_column_in_history_mode update_new", sql.str(),
              "Could not update new rows for drop_column_in_history_mode");
  }
  {
    // Query 3: Update previous active records to mark them inactive
    std::ostringstream sql;
    sql << "UPDATE " << absolute_table_name
        << " SET \"_fivetran_active\" = FALSE,"
        << " \"_fivetran_end\" = (" << quoted_timestamp
        << "::TIMESTAMP - (INTERVAL '1 millisecond'))"
        << " WHERE \"_fivetran_active\" = TRUE"
        << " AND " << quoted_column << " IS NOT NULL"
        << " AND \"_fivetran_start\" < " << quoted_timestamp;

    run_query(
        con, "drop_column_in_history_mode update_prev", sql.str(),
        "Could not update previous records for drop_column_in_history_mode");
  }

  con.Commit();
}

void MdSqlGenerator::copy_table(duckdb::Connection &con,
                                const table_def &from_table,
                                const table_def &to_table) {
  con.BeginTransaction();

  {
    std::ostringstream sql;
    sql << "CREATE TABLE " << to_table.to_escaped_string()
        << " AS SELECT * FROM " << from_table.to_escaped_string();

    run_query(con, "copy_table", sql.str(),
              "Could not copy table <" + from_table.to_escaped_string() +
                  "> to <" + to_table.to_escaped_string() + ">");
  }

  const auto columns = describe_table(con, from_table);

  std::vector<const column_def *> columns_pk;
  std::vector<const column_def *> columns_regular;
  find_primary_keys(columns, columns_pk, &columns_regular, "_fivetran_start");

  for (const auto &column : columns) {
    if (column.column_default.empty() || column.column_default == "NULL") {
      continue;
    }

    std::ostringstream sql;
    sql << "ALTER TABLE " << to_table.to_escaped_string() << " ALTER COLUMN "
        << KeywordHelper::WriteQuoted(column.name, '"') << " SET DEFAULT "
        << KeywordHelper::WriteQuoted(column.column_default, '\'') << ";";
    run_query(con, "copy_table set_default", sql.str(),
              "Could not add default to column " + column.name);
  }

  if (!columns_pk.empty()) {
    // Note that "CREATE TABLE AS SELECT" does not add any primary key
    // constraints.
    std::ostringstream alter_sql;

    alter_sql << "ALTER TABLE " << to_table.to_escaped_string()
              << " ADD PRIMARY KEY (";
    write_joined(alter_sql, columns_pk, print_column);
    alter_sql << ");";
    run_query(con, "migrate_history_to_live alter", alter_sql.str(),
              "Could not alter soft_delete table");
  }

  con.Commit();
}

void MdSqlGenerator::copy_column(duckdb::Connection &con,
                                 const table_def &table,
                                 const std::string &from_column,
                                 const std::string &to_column) {
  const std::string absolute_table_name = table.to_escaped_string();
  const std::string quoted_from = KeywordHelper::WriteQuoted(from_column, '"');
  const std::string quoted_to = KeywordHelper::WriteQuoted(to_column, '"');

  // Get the column type from the source column
  auto columns = describe_table(con, table);
  column_def *source_col = nullptr;
  for (auto &col : columns) {
    if (col.name == from_column) {
      source_col = &col;
      break;
    }
  }
  if (!source_col) {
    throw std::runtime_error("Source column <" + from_column + "> not found");
  }

  // Add the new column
  std::ostringstream add_sql;
  add_sql << "ALTER TABLE " << absolute_table_name << " ADD COLUMN "
          << quoted_to << " " << format_type(*source_col);

  con.BeginTransaction();

  run_query(con, "copy_column add", add_sql.str(),
            "Could not add column for copy_column");

  // Copy values
  std::ostringstream update_sql;
  update_sql << "UPDATE " << absolute_table_name << " SET " << quoted_to
             << " = " << quoted_from;
  run_query(con, "copy_column update", update_sql.str(),
            "Could not copy column values");

  con.Commit();
}

void MdSqlGenerator::copy_table_to_history_mode(
    duckdb::Connection &con, const table_def &from_table,
    const table_def &to_table, const std::string &soft_deleted_column) {
  const std::string from_table_name = from_table.to_escaped_string();
  const std::string to_table_name = to_table.to_escaped_string();

  table_def temp_table{to_table.db_name, to_table.schema_name,
                       to_table.table_name + "_temp"};
  const std::string temp_absolute_table_name = temp_table.to_escaped_string();

  std::vector<const column_def *> columns_pk;
  std::vector<const column_def *> columns_regular;
  const auto columns = describe_table(con, from_table);
  find_primary_keys(columns, columns_pk, &columns_regular);

  con.BeginTransaction();
  // Create new table with data and history columns in one statement
  {
    std::ostringstream sql;
    sql << "CREATE TABLE " << to_table_name << " AS FROM " << from_table_name;

    run_query(con, "copy_table_to_history_mode", sql.str(),
              "Could not copy table to history mode");
  }

  for (const auto &column : columns) {
    if (column.column_default.empty() || column.column_default == "NULL") {
      continue;
    }

    if (column.name == "_fivetran_deleted") {
      continue;
    }

    std::ostringstream sql;
    sql << "ALTER TABLE " << to_table_name << " ALTER COLUMN "
        << KeywordHelper::WriteQuoted(column.name, '"') << " SET DEFAULT "
        << KeywordHelper::WriteQuoted(column.column_default, '\'') << ";";
    run_query(con, "copy_table_to_history_mode set_default", sql.str(),
              "Could not add default to column " + column.name);
  }

  if (!columns_pk.empty()) {
    // Add the right primary key. Note that "CREATE TABLE AS SELECT" does not
    // add any primary key constraints.
    std::ostringstream sql;

    sql << "ALTER TABLE " << to_table_name << " ADD PRIMARY KEY (";
    write_joined(sql, columns_pk, print_column);
    sql << ");";
    run_query(con, "copy_table_to_history_mode alter", sql.str(),
              "Could not alter soft_delete table");
  }
  con.Commit();

  if (!soft_deleted_column.empty()) {
    migrate_soft_delete_to_history(con, to_table, soft_deleted_column);
  } else {
    migrate_live_to_history(con, to_table);
  }
}

void MdSqlGenerator::rename_table(duckdb::Connection &con,
                                  const table_def &from_table,
                                  const std::string &to_table_name) {
  std::ostringstream sql;
  sql << "ALTER TABLE " << from_table.to_escaped_string() << " RENAME TO "
      << KeywordHelper::WriteQuoted(to_table_name, '"');

  run_query(con, "rename_table", sql.str(),
            "Could not rename table <" + from_table.to_escaped_string() + ">");
}

void MdSqlGenerator::rename_column(duckdb::Connection &con,
                                   const table_def &table,
                                   const std::string &from_column,
                                   const std::string &to_column) {
  const std::string absolute_table_name = table.to_escaped_string();
  std::ostringstream sql;
  sql << "ALTER TABLE " << absolute_table_name << " RENAME COLUMN "
      << KeywordHelper::WriteQuoted(from_column, '"') << " TO "
      << KeywordHelper::WriteQuoted(to_column, '"');

  run_query(con, "rename_column", sql.str(),
            "Could not rename column <" + from_column + "> to <" + to_column +
                "> in table <" + absolute_table_name + ">");
}

void MdSqlGenerator::add_column_with_default(duckdb::Connection &con,
                                             const table_def &table,
                                             const std::string &column,
                                             fivetran_sdk::v2::DataType type,
                                             const std::string &default_value) {
  const std::string absolute_table_name = table.to_escaped_string();
  const std::string quoted_column = KeywordHelper::WriteQuoted(column, '"');
  const std::string type_str = fivetran_type_to_duckdb_type_string(type);
  const std::string casted_default_value =
      "CAST(" + KeywordHelper::WriteQuoted(default_value, '\'') + " AS " +
      type_str + ")";

  std::ostringstream sql;
  sql << "ALTER TABLE " << absolute_table_name << " ADD COLUMN "
      << quoted_column << " " << type_str;

  if (!default_value.empty()) {
    sql << " DEFAULT " << casted_default_value;
  }

  run_query(con, "add_column_with_default", sql.str(),
            "Could not add column <" + column + "> to table <" +
                absolute_table_name + ">");
}

bool MdSqlGenerator::validate_history_table(
    duckdb::Connection &con, const std::string absolute_table_name,
    const std::string quoted_timestamp) {
  // This performs the "Validation before starting the migration" part of
  // add/drop column in history mode as specified in the docs:
  // https://github.com/fivetran/fivetran_partner_sdk/blob/bdaea1a/schema-migration-helper-service.md
  auto result = con.Query("SELECT COUNT(*) FROM " + absolute_table_name);

  if (result->HasError()) {
    throw std::runtime_error("Could not query table size");
  }

  if (result->GetValue(0, 0) == 0) {
    // The table is empty
    return true;
  }

  auto max_result = con.Query(
      "SELECT MAX(\"_fivetran_start\") <= " + quoted_timestamp + " FROM " +
      absolute_table_name + " WHERE \"_fivetran_active\" = true");

  if (max_result->HasError()) {
    throw std::runtime_error("Could not query _fivetran_start value");
  }

  if (max_result->GetValue(0, 0) != true) {
    throw std::runtime_error("_fivetran_start column contains values larger "
                             "than the operation timestamp");
  }

  return false;
}

void MdSqlGenerator::add_column_in_history_mode(
    duckdb::Connection &con, const table_def &table, const std::string &column,
    fivetran_sdk::v2::DataType type, const std::string &default_value,
    const std::string &operation_timestamp) {
  const std::string absolute_table_name = table.to_escaped_string();
  const std::string quoted_column = KeywordHelper::WriteQuoted(column, '"');
  const std::string type_str = fivetran_type_to_duckdb_type_string(type);
  const std::string casted_default_value =
      "CAST(" + KeywordHelper::WriteQuoted(default_value, '\'') + " AS " +
      type_str + ")";
  const std::string quoted_timestamp =
      "'" + operation_timestamp + "'::TIMESTAMPTZ";

  if (validate_history_table(con, absolute_table_name, quoted_timestamp)) {
    return;
  }

  // Add the column
  std::ostringstream add_sql;
  add_sql << "ALTER TABLE " << absolute_table_name << " ADD COLUMN "
          << quoted_column << " " << type_str;

  con.BeginTransaction();

  run_query(con, "add_column_in_history_mode add", add_sql.str(),
            "Could not add column for add_column_in_history_mode");

  // Insert new rows with the default value, capturing the DDL change
  std::ostringstream insert_sql;
  insert_sql << "INSERT INTO " << absolute_table_name << " SELECT * REPLACE ("
             << casted_default_value << " AS " << quoted_column << ", "
             << quoted_timestamp << " AS \"_fivetran_start\")"
             << " FROM " << absolute_table_name
             << " WHERE \"_fivetran_active\" = TRUE AND _fivetran_start < "
             << quoted_timestamp;

  run_query(con, "add_column_in_history_mode insert", insert_sql.str(),
            "Could not insert new rows for add_column_in_history_mode");

  // Per the docs:
  //   This step is important in case of source connector sends multiple
  //   ADD_COLUMN_IN_HISTORY_MODE operations with the same operation_timestamp.
  //   It will ensure, we only record history once for that timestamp.
  // Also see drop_column_in_history_mode(): this ensures that if we already
  // inserted records for the current operation_timestamp, we set the right
  // default value for the column we're currently processing.

  std::ostringstream update_new_sql;
  update_new_sql << "UPDATE " << absolute_table_name << " SET " << quoted_column
                 << " = " << casted_default_value
                 << " WHERE \"_fivetran_start\" = " << quoted_timestamp;

  run_query(con, "add_column_in_history_mode update_new", update_new_sql.str(),
            "Could not update new rows for add_column_in_history_mode");

  // Update previous active records
  std::ostringstream update_prev_sql;
  update_prev_sql << "UPDATE " << absolute_table_name
                  << " SET \"_fivetran_active\" = FALSE,"
                  << " \"_fivetran_end\" = (" << quoted_timestamp
                  << "::TIMESTAMP - (INTERVAL '1 millisecond'))"
                  << " WHERE \"_fivetran_active\" = TRUE"
                  << " AND \"_fivetran_start\" < " << quoted_timestamp;

  run_query(con, "add_column_in_history_mode update", update_prev_sql.str(),
            "Could not update records for add_column_in_history_mode");

  con.Commit();
}

void MdSqlGenerator::update_column_value(duckdb::Connection &con,
                                         const table_def &table,
                                         const std::string &column,
                                         const std::string &value) {
  const std::string absolute_table_name = table.to_escaped_string();
  const std::string quoted_column = KeywordHelper::WriteQuoted(column, '"');

  std::ostringstream sql;
  sql << "UPDATE " << absolute_table_name << " SET " << quoted_column << " = "
      << value;

  run_query(con, "update_column_value", sql.str(),
            "Could not update column <" + column + "> in table <" +
                absolute_table_name + ">");
}

void MdSqlGenerator::migrate_soft_delete_to_live(
    duckdb::Connection &con, const table_def &table,
    const std::string &soft_deleted_column) {
  const std::string absolute_table_name = table.to_escaped_string();
  const std::string quoted_deleted_col =
      KeywordHelper::WriteQuoted(soft_deleted_column, '"');

  // Delete rows where soft_deleted_column = TRUE
  std::ostringstream delete_sql;
  delete_sql << "DELETE FROM " << absolute_table_name << " WHERE "
             << quoted_deleted_col << " = TRUE";
  run_query(con, "migrate_soft_delete_to_live delete", delete_sql.str(),
            "Could not delete soft-deleted rows");

  // Drop the soft_deleted_column if it's _fivetran_deleted
  if (soft_deleted_column == "_fivetran_deleted") {
    std::ostringstream drop_sql;
    drop_sql << "ALTER TABLE " << absolute_table_name << " DROP COLUMN "
             << quoted_deleted_col;
    run_query(con, "migrate_soft_delete_to_live drop", drop_sql.str(),
              "Could not drop soft_deleted_column");
  }
}

void MdSqlGenerator::migrate_soft_delete_to_history(
    duckdb::Connection &con, const table_def &table,
    const std::string &soft_deleted_column) {
  const std::string absolute_table_name = table.to_escaped_string();
  const std::string quoted_deleted_col =
      KeywordHelper::WriteQuoted(soft_deleted_column, '"');

  table_def temp_table{table.db_name, table.schema_name,
                       table.table_name + "_temp"};
  const std::string temp_absolute_table_name = temp_table.to_escaped_string();

  std::vector<const column_def *> columns_pk;
  std::vector<const column_def *> columns_regular;
  const auto columns = describe_table(con, table);
  find_primary_keys(columns, columns_pk, &columns_regular);

  run_query(con, "migrate_soft_delete_to_history add_start",
            "ALTER TABLE " + absolute_table_name +
                " ADD COLUMN \"_fivetran_start\" TIMESTAMPTZ;",
            "Could not add _fivetran_start column");
  run_query(con, "migrate_soft_delete_to_history add_end",
            "ALTER TABLE " + absolute_table_name +
                " ADD COLUMN \"_fivetran_end\" TIMESTAMPTZ;",
            "Could not add _fivetran_end column");
  run_query(con, "migrate_soft_delete_to_history add_active",
            "ALTER TABLE " + absolute_table_name +
                " ADD COLUMN \"_fivetran_active\" BOOLEAN DEFAULT TRUE;",
            "Could not add _fivetran_active column");

  // Set values based on soft_deleted_column
  constexpr auto query_template = R"(
  UPDATE {0}
  SET
    "_fivetran_active" = COALESCE(NOT {1}, TRUE),
    "_fivetran_start" = CASE
        WHEN {1} = TRUE THEN 'epoch'::TIMESTAMPTZ
        ELSE (SELECT MAX("_fivetran_synced") FROM {0})
    END,
    "_fivetran_end" = CASE
      WHEN {1} = TRUE THEN 'epoch'::TIMESTAMPTZ
      ELSE '9999-12-31T23:59:59.999Z'::TIMESTAMPTZ
    END;
  )";
  std::string update_sql =
      std::format(query_template, absolute_table_name, quoted_deleted_col);
  run_query(con, "migrate_soft_delete_to_history update", update_sql,
            "Could not set history column values");

  con.BeginTransaction(); // See duckdb issue #20570: we can only start the
                          // transaction here at this point.

  // Drop the soft_deleted_column if it's _fivetran_deleted
  if (soft_deleted_column == "_fivetran_deleted") {
    std::ostringstream drop_sql;
    drop_sql << "ALTER TABLE " << absolute_table_name << " DROP COLUMN "
             << quoted_deleted_col << ";";
    run_query(con, "migrate_soft_delete_to_history drop", drop_sql.str(),
              "Could not drop soft_deleted_column");
  }

  // Rename, copy and drop the original table to be able to replace the primary
  // key
  run_query(con, "migrate_soft_delete_to_history rename",
            "ALTER TABLE " + absolute_table_name + " RENAME TO " +
                KeywordHelper::WriteQuoted(temp_table.table_name, '"') + ";",
            "Could rename original soft_delete table");
  run_query(con, "migrate_soft_delete_to_history copy",
            "CREATE TABLE " + absolute_table_name + " AS SELECT * FROM " +
                temp_absolute_table_name + ";",
            "Could not create new table from temp table");
  run_query(con, "migrate_soft_delete_to_history drop",
            "DROP TABLE " + temp_absolute_table_name + ";",
            "Could not drop temp table");

  for (const auto &column : columns) {
    // This also sets the default value of the soft_deleted_column if it was not
    // equal to _fivetran_deleted
    if (column.column_default.empty() || column.column_default == "NULL") {
      continue;
    }
    if (column.name == "_fivetran_deleted") {
      continue;
    }

    std::ostringstream sql;
    sql << "ALTER TABLE " << absolute_table_name << " ALTER COLUMN "
        << KeywordHelper::WriteQuoted(column.name, '"') << " SET DEFAULT "
        << KeywordHelper::WriteQuoted(column.column_default, '\'') << ";";
    run_query(con, "migrate_soft_delete_to_history set_default", sql.str(),
              "Could not add default to column " + column.name);
  }

  if (!columns_pk.empty()) {
    column_def fivetran_start{.name = "_fivetran_start",
                              .type = duckdb::LogicalTypeId::TIMESTAMP_TZ};
    columns_pk.push_back(&fivetran_start);

    // Add the right primary key. Note that "CREATE TABLE AS SELECT" does not
    // add any primary key constraints.
    std::ostringstream sql;

    sql << "ALTER TABLE " << absolute_table_name << " ADD PRIMARY KEY (";
    write_joined(sql, columns_pk, print_column);
    sql << ");";
    run_query(con, "migrate_soft_delete_to_history alter", sql.str(),
              "Could not alter soft_delete table");
  }
  con.HasActiveTransaction();
  con.Commit();
}

void MdSqlGenerator::migrate_history_to_soft_delete(
    duckdb::Connection &con, const table_def &table,
    const std::string &soft_deleted_column) {
  const std::string absolute_table_name = table.to_escaped_string();
  const std::string quoted_deleted_col =
      KeywordHelper::WriteQuoted(soft_deleted_column, '"');

  con.BeginTransaction(); // Avoid creating hanging tables

  // From the duckdb docs:
  // "ADD CONSTRAINT and DROP CONSTRAINT clauses are not yet supported in
  // DuckDB." In particular, we cannot drop the primary key constraint from the
  // original table. Hence, we need to create a new table.
  table_def temp_table{table.db_name, table.schema_name,
                       table.table_name + "_temp"};
  const std::string temp_absolute_table_name = temp_table.to_escaped_string();

  std::vector<const column_def *> columns_pk;
  std::vector<const column_def *> columns_regular;
  const auto columns = describe_table(con, table);
  find_primary_keys(columns, columns_pk, &columns_regular, "_fivetran_start");

  {
    std::ostringstream sql;
    sql << "CREATE TABLE " << temp_absolute_table_name
        << " AS SELECT * EXCLUDE (\"_fivetran_start\", \"_fivetran_end\") FROM "
        << absolute_table_name;

    if (!columns_pk.empty()) {
      // Keep only the latest record for a primary key based on the highest
      // _fivetran_start, using QUALIFY
      sql << " QUALIFY row_number() OVER (partition by ";
      write_joined(sql, columns_pk, print_column);
      sql << " ORDER BY \"_fivetran_start\" DESC) = 1";
    }

    run_query(con, "migrate_history_to_soft_delete create", sql.str(),
              "Could not add soft_deleted_column");
  }

  {
    std::ostringstream sql;
    sql << "ALTER TABLE " << temp_absolute_table_name
        << " ADD COLUMN IF NOT EXISTS \"_fivetran_deleted\" BOOLEAN "
           "DEFAULT false;";
    run_query(con, "migrate_history_to_soft_delete add_col", sql.str(),
              "Could not add column _fivetran_deleted");
  }

  {
    std::ostringstream sql;
    sql << "UPDATE " << temp_absolute_table_name << " SET "
        << quoted_deleted_col << " = NOT \"_fivetran_active\";";
    run_query(con, "migrate_history_to_soft_delete update_soft_deleted",
              sql.str(), "Could not update soft_deleted_column");
  }
  {
    std::ostringstream sql;
    sql << "ALTER TABLE " << temp_absolute_table_name
        << " DROP COLUMN \"_fivetran_active\";";
    run_query(con, "migrate_history_to_soft_delete drop_active", sql.str(),
              "Could not drop _fivetran_active column");
  }

  for (const auto &column : columns) {
    // This also sets the default value of the soft_deleted_column if it was not
    // equal to _fivetran_deleted
    if (column.column_default.empty() || column.column_default == "NULL") {
      continue;
    }
    if (column.name == "_fivetran_start" || column.name == "_fivetran_end" ||
        column.name == "_fivetran_active" ||
        column.name == "_fivetran_deleted") {
      continue;
    }

    std::ostringstream sql;
    sql << "ALTER TABLE " << temp_absolute_table_name << " ALTER COLUMN "
        << KeywordHelper::WriteQuoted(column.name, '"') << " SET DEFAULT "
        << KeywordHelper::WriteQuoted(column.column_default, '\'') << ";";
    run_query(con, "migrate_history_to_soft_delete set_default", sql.str(),
              "Could not add default to column " + column.name);
  }

  if (!columns_pk.empty()) {
    // Add the right primary key. Note that "CREATE TABLE AS SELECT" does not
    // add any primary key constraints.
    std::ostringstream sql;

    sql << "ALTER TABLE " << temp_absolute_table_name << " ADD PRIMARY KEY (";
    write_joined(sql, columns_pk, print_column);
    sql << ");";
    run_query(con, "migrate_history_to_soft_delete alter", sql.str(),
              "Could not alter soft_delete table");
  }

  // Swap the original and temporary table
  run_query(con, "migrate_history_to_soft_delete drop",
            "DROP TABLE " + absolute_table_name,
            "Could not drop original soft_delete table");
  run_query(con, "migrate_history_to_soft_delete rename",
            "ALTER TABLE " + temp_absolute_table_name + " RENAME TO " +
                KeywordHelper::WriteQuoted(table.table_name, '"'),
            "Could not rename temp table to soft_delete table");

  con.Commit();
}

void MdSqlGenerator::migrate_history_to_live(duckdb::Connection &con,
                                             const table_def &table,
                                             bool keep_deleted_rows) {
  const std::string absolute_table_name = table.to_escaped_string();

  con.BeginTransaction(); // Avoid creating hanging tables

  // Optionally delete inactive rows
  if (!keep_deleted_rows) {
    run_query(con, "migrate_history_to_live delete",
              "DELETE FROM " + absolute_table_name +
                  " WHERE \"_fivetran_active\" = FALSE",
              "Could not delete inactive rows");
  }

  // From the duckdb docs:
  // "ADD CONSTRAINT and DROP CONSTRAINT clauses are not yet supported in
  // DuckDB." In particular, we cannot drop the primary key constraint from the
  // original table. Hence, we need to create a new table.
  table_def temp_table{table.db_name, table.schema_name,
                       table.table_name + "_temp"};
  const std::string temp_absolute_table_name = temp_table.to_escaped_string();

  // Combine steps 1 and 3
  std::ostringstream add_sql;
  add_sql << "CREATE TABLE " << temp_absolute_table_name
          << " AS SELECT * EXCLUDE (\"_fivetran_start\", \"_fivetran_end\", "
             "\"_fivetran_active\") "
          << " FROM " << absolute_table_name << ";";
  run_query(con, "migrate_history_to_live create", add_sql.str(),
            "Could not add soft_deleted_column");

  const auto columns = describe_table(con, table);

  std::vector<const column_def *> columns_pk;
  std::vector<const column_def *> columns_regular;
  find_primary_keys(columns, columns_pk, &columns_regular, "_fivetran_start");

  for (const auto &column : columns) {
    if (column.column_default.empty() || column.column_default == "NULL") {
      continue;
    }
    if (column.name == "_fivetran_start" || column.name == "_fivetran_end" ||
        column.name == "_fivetran_active") {
      continue;
    }

    std::ostringstream sql;
    sql << "ALTER TABLE " << temp_absolute_table_name << " ALTER COLUMN "
        << KeywordHelper::WriteQuoted(column.name, '"') << " SET DEFAULT "
        << KeywordHelper::WriteQuoted(column.column_default, '\'') << ";";
    run_query(con, "migrate_history_to_live set_default", sql.str(),
              "Could not add default to column " + column.name);
  }

  if (!columns_pk.empty() &&
      !keep_deleted_rows) { // We can't set the original primary key if we keep
                            // duplicates...
    // Add the right primary key. Note that "CREATE TABLE AS SELECT" does not
    // add any primary key constraints.
    std::ostringstream alter_sql;

    alter_sql << "ALTER TABLE " << temp_absolute_table_name
              << " ADD PRIMARY KEY (";
    write_joined(alter_sql, columns_pk, print_column);
    alter_sql << ");";
    run_query(con, "migrate_history_to_live alter", alter_sql.str(),
              "Could not alter soft_delete table");
  }

  // Swap the original and temporary table
  run_query(con, "migrate_history_to_live drop",
            "DROP TABLE " + absolute_table_name,
            "Could not drop original soft_delete table");
  run_query(con, "migrate_history_to_live rename",
            "ALTER TABLE " + temp_absolute_table_name + " RENAME TO " +
                KeywordHelper::WriteQuoted(table.table_name, '"'),
            "Could not rename temp table to soft_delete table");

  con.Commit();
}

void MdSqlGenerator::migrate_live_to_soft_delete(
    duckdb::Connection &con, const table_def &table,
    const std::string &soft_deleted_column) {
  const std::string absolute_table_name = table.to_escaped_string();
  const std::string quoted_deleted_col =
      KeywordHelper::WriteQuoted(soft_deleted_column, '"');

  // Add soft_deleted_column if it doesn't exist
  std::ostringstream add_sql;
  add_sql << "ALTER TABLE " << absolute_table_name
          << " ADD COLUMN IF NOT EXISTS " << quoted_deleted_col << " BOOLEAN;";
  run_query(con, "migrate_live_to_soft_delete add", add_sql.str(),
            "Could not add soft_deleted_column");

  // Set all existing rows to not deleted
  std::ostringstream update_sql;
  update_sql << "UPDATE " << absolute_table_name << " SET "
             << quoted_deleted_col << " = FALSE WHERE " << quoted_deleted_col
             << " IS NULL";
  run_query(con, "migrate_live_to_soft_delete update", update_sql.str(),
            "Could not set soft_deleted_column values");
}

void MdSqlGenerator::migrate_live_to_history(duckdb::Connection &con,
                                             const table_def &table) {
  const std::string absolute_table_name = table.to_escaped_string();
  table_def temp_table{table.db_name, table.schema_name,
                       table.table_name + "_temp"};
  const std::string temp_absolute_table_name = temp_table.to_escaped_string();

  std::vector<const column_def *> columns_pk;
  std::vector<const column_def *> columns_regular;
  const auto columns = describe_table(con, table);
  find_primary_keys(columns, columns_pk, &columns_regular);

  con.BeginTransaction();

  run_query(con, "migrate_live_to_history add_start",
            "ALTER TABLE " + absolute_table_name +
                " ADD COLUMN \"_fivetran_start\" TIMESTAMPTZ",
            "Could not add _fivetran_start column");
  run_query(con, "migrate_live_to_history add_end",
            "ALTER TABLE " + absolute_table_name +
                " ADD COLUMN \"_fivetran_end\" TIMESTAMPTZ",
            "Could not add _fivetran_end column");
  run_query(con, "migrate_live_to_history add_active",
            "ALTER TABLE " + absolute_table_name +
                " ADD COLUMN \"_fivetran_active\" BOOLEAN DEFAULT TRUE",
            "Could not add _fivetran_active column");

  // Set all records as active
  run_query(con, "migrate_live_to_history update",
            "UPDATE " + absolute_table_name +
                " SET \"_fivetran_start\" = NOW(),"
                " \"_fivetran_end\" = '9999-12-31T23:59:59.999Z'::TIMESTAMPTZ,"
                " \"_fivetran_active\" = TRUE",
            "Could not set history column values");

  // Rename, copy and drop the original table to be able to replace the primary
  // key
  run_query(con, "migrate_live_to_history rename",
            "ALTER TABLE " + absolute_table_name + " RENAME TO " +
                KeywordHelper::WriteQuoted(temp_table.table_name, '"'),
            "Could not drop original soft_delete table");
  run_query(con, "migrate_live_to_history copy",
            "CREATE TABLE " + absolute_table_name + " AS SELECT * FROM " +
                temp_absolute_table_name,
            "Could not rename temp table to soft_delete table");
  run_query(con, "migrate_live_to_history drop",
            "DROP TABLE " + temp_absolute_table_name,
            "Could not rename temp table to soft_delete table");

  for (const auto &column : columns) {
    // This also sets the default value of the soft_deleted_column if it was not
    // equal to _fivetran_deleted
    if (column.column_default.empty() || column.column_default == "NULL") {
      continue;
    }

    std::ostringstream sql;
    sql << "ALTER TABLE " << absolute_table_name << " ALTER COLUMN "
        << KeywordHelper::WriteQuoted(column.name, '"') << " SET DEFAULT "
        << KeywordHelper::WriteQuoted(column.column_default, '\'') << ";";
    run_query(con, "migrate_live_to_history set_default", sql.str(),
              "Could not add default to column " + column.name);
  }

  if (!columns_pk.empty()) {
    column_def fivetran_start{.name = "_fivetran_start",
                              .type = duckdb::LogicalTypeId::TIMESTAMP_TZ};
    columns_pk.push_back(&fivetran_start);
    // Add the right primary key. Note that "CREATE TABLE AS SELECT" does not
    // add any primary key constraints.
    std::ostringstream sql;

    sql << "ALTER TABLE " << absolute_table_name << " ADD PRIMARY KEY (";
    write_joined(sql, columns_pk, print_column);
    sql << ");";
    run_query(con, "migrate_live_to_history alter", sql.str(),
              "Could not alter soft_delete table");
  }
  con.Commit();
}
