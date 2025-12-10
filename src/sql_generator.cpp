#include <iostream>
#include <md_logging.hpp>
#include <string>

#include "../includes/sql_generator.hpp"

using duckdb::KeywordHelper;

// Utility

std::ostream &operator<<(std::ostream &os, const column_def &col) {
  os << duckdb::EnumUtil::ToChars(col.type);
  if (col.type == duckdb::LogicalTypeId::DECIMAL) {
    os << " (" << col.width << "," << col.scale << ")";
  }
  return os;
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
void create_latest_active_records_table(
    duckdb::Connection &con, const std::string &absolute_table_name) {
  con.Query(
      "CREATE TABLE IF NOT EXISTS latest_active_records AS (SELECT * FROM " +
      absolute_table_name + ") LIMIT 0");
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
    ddl << KeywordHelper::WriteQuoted(col.name, '"') << " " << col;
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

  auto &materialized_result = result->Cast<duckdb::MaterializedQueryResult>();

  for (const auto &row : materialized_result.Collection().GetRows()) {
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
    const std::set<std::string> &alter_types,
    const std::map<std::string, column_def> &new_column_map) {
  for (const auto &col : added_columns) {
    std::ostringstream out;
    out << "ALTER TABLE " << absolute_table_name << " ADD COLUMN "
        << KeywordHelper::WriteQuoted(col.name, '"') << " " << col;

    run_query(con, "alter_table add", out.str(),
              "Could not add column <" + col.name + "> to table <" +
                  absolute_table_name + ">");
  }

  for (const auto &col_name : alter_types) {
    std::ostringstream out;
    out << "ALTER TABLE " << absolute_table_name << " ALTER ";
    const auto &col = new_column_map.at(col_name);

    out << KeywordHelper::WriteQuoted(col_name, '"') << " TYPE " << col;

    run_query(con, "alter table change type", out.str(),
              "Could not alter type for column <" + col_name + "> in table <" +
                  absolute_table_name + ">");
  }
}

void MdSqlGenerator::alter_table(
    duckdb::Connection &con, const table_def &table,
    const std::vector<column_def> &requested_columns) {

  bool recreate_table = false;

  auto absolute_table_name = table.to_escaped_string();
  std::set<std::string> alter_types;
  std::set<std::string> added_columns;
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
      logger->info("Source connector requested that table " +
                   absolute_table_name + " column " + col.name +
                   " be dropped, but dropping columns is not allowed");
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
                         alter_types, new_column_map);
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

void MdSqlGenerator::add_partial_historical_values(
    duckdb::Connection &con, const table_def &table,
    const std::string &staging_table_name,
    std::vector<const column_def *> &columns_pk,
    std::vector<const column_def *> &columns_regular,
    const std::string &unmodified_string) {
  std::ostringstream sql;
  auto absolute_table_name = table.to_escaped_string();

  // create empty table structure just in case there were not any earliest files
  // that would have created it
  create_latest_active_records_table(con, absolute_table_name);

  sql << "INSERT INTO " << absolute_table_name << " ( SELECT ";

  // use primary keys as is, without checking for unmodified value
  write_joined(
      sql, columns_pk,
      [&](const std::string &quoted_col, std::ostringstream &out) {
        out << staging_table_name << "." << quoted_col;
      },
      ", ");
  sql << ",  ";

  write_joined(sql, columns_regular,
               [staging_table_name, absolute_table_name, unmodified_string](
                   const std::string quoted_col, std::ostringstream &out) {
                 out << "CASE WHEN " << staging_table_name << "." << quoted_col
                     << " = "
                     << KeywordHelper::WriteQuoted(unmodified_string, '\'')
                     << " THEN lar." << quoted_col << " ELSE "
                     << staging_table_name << "." << quoted_col << " END as "
                     << quoted_col;
               });

  sql << " FROM " << staging_table_name
      << " LEFT JOIN latest_active_records lar ON "
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
    std::vector<const column_def *> &columns_pk) {

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

  create_latest_active_records_table(con, absolute_table_name);

  {
    // store latest versions of records before they get deactivated
    // per spec, this should be limited to _fivetran_active = TRUE but it's
    // safer to get all latest versions even if deactivated to prevent null
    // values in  a partially successful batch.
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

    sql << "INSERT INTO latest_active_records SELECT * EXCLUDE (row_num) FROM "
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

void MdSqlGenerator::check_connection(duckdb::Connection &con) {
  auto result = con.Query("PRAGMA MD_VERSION");
  if (result->HasError()) {
    throw std::runtime_error("Error checking connection: " +
                             result->GetError());
  }
}
