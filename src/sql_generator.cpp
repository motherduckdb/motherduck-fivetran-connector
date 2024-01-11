#include <iostream>
#include <logging.hpp>
#include <string>

#include "../includes/sql_generator.hpp"

using duckdb::KeywordHelper;

// Utility
std::string compute_absolute_table_name(const std::string &db_name,
                                        const std::string &schema_name,
                                        const std::string &table_name) {
  std::ostringstream out;
  out << KeywordHelper::WriteQuoted(db_name, '"') << "."
      << KeywordHelper::WriteQuoted(schema_name, '"') << "."
      << KeywordHelper::WriteQuoted(table_name, '"');
  return out.str();
}

void write_joined(
    std::ostringstream &sql, const std::vector<column_def> &columns,
    std::function<void(const column_def &, std::ostringstream &)> print_col) {
  bool first = true;
  for (const auto &col : columns) {
    if (!col.primary_key) {
      if (first) {
        first = false;
      } else {
        sql << ", ";
      }
      print_col(col, sql);
    }
  }
}

void write_joined(
    std::ostringstream &sql, const std::vector<std::string> &strings,
    std::function<void(const std::string &, std::ostringstream &)> print_str) {
  bool first = true;
  for (const auto &str : strings) {
    if (first) {
      first = false;
    } else {
      sql << ", ";
    }
    print_str(str, sql);
  }
}

// DuckDB querying
bool schema_exists(duckdb::Connection &con, const std::string &db_name,
                   const std::string &schema_name) {
  std::ostringstream out;
  out << "SELECT schema_name FROM information_schema.schemata WHERE "
         "catalog_name="
      << KeywordHelper::WriteQuoted(db_name, '\'')
      << " AND schema_name=" << KeywordHelper::WriteQuoted(schema_name, '\'');

  auto query = out.str();
  mdlog::info("schema_exists: " + query);
  auto result = con.Query(query);

  if (result->HasError()) {
    throw std::runtime_error("Could not find whether schema exists: " +
                             result->GetError());
  }
  return result->RowCount() > 0;
}

bool table_exists(duckdb::Connection &con, const std::string &db_name,
                  const std::string &schema_name,
                  const std::string &table_name) {
  std::ostringstream out;
  out << "SELECT table_name FROM information_schema.tables WHERE table_catalog="
      << KeywordHelper::WriteQuoted(db_name, '\'')
      << " AND table_schema=" << KeywordHelper::WriteQuoted(schema_name, '\'')
      << " AND table_name=" << KeywordHelper::WriteQuoted(table_name, '\'');

  auto query = out.str();
  mdlog::info("table_exists: " + query);
  auto result = con.Query(query);

  if (result->HasError()) {
    throw std::runtime_error("Could not find whether table exists: " +
                             result->GetError());
  }

  return result->RowCount() > 0;
}

void create_schema(duckdb::Connection &con, const std::string &db_name,
                   const std::string &schema_name) {
  auto query = "CREATE schema " + KeywordHelper::WriteQuoted(schema_name, '\'');
  mdlog::info("create_schema: " + query);
  con.Query(query);
}

void create_table(duckdb::Connection &con, const std::string &db_name,
                  const std::string &schema_name, const std::string &table_name,
                  const std::vector<column_def> &columns) {
  std::ostringstream ddl;
  ddl << "CREATE OR REPLACE TABLE "
      << compute_absolute_table_name(db_name, schema_name, table_name) << " (";

  for (const auto &col : columns) {
    ddl << KeywordHelper::WriteQuoted(col.name, '"') << " "
        << duckdb::EnumUtil::ToChars(col.type);
    if (col.primary_key) {
      ddl << " PRIMARY KEY";
    }
    ddl << ", "; // DuckDB allows trailing commas
  }

  ddl << ")";

  auto query = ddl.str();
  mdlog::info("create_table: " + query);

  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error(result->GetError());
  }
}

std::vector<column_def> describe_table(duckdb::Connection &con,
                                       const std::string &db_name,
                                       const std::string &schema_name,
                                       const std::string &table_name) {
  // TBD is_identity is never set, used is_nullable=no temporarily but really
  // should use duckdb_constraints table.
  std::ostringstream sql;
  sql << "SELECT column_name, data_type, is_nullable == 'NO' FROM "
         "information_schema.columns WHERE table_catalog="
      << KeywordHelper::WriteQuoted(db_name, '\'')
      << " AND table_schema=" << KeywordHelper::WriteQuoted(schema_name, '\'')
      << " AND table_name=" << KeywordHelper::WriteQuoted(table_name, '\'');

  // TBD scale/precision
  std::vector<column_def> columns;

  auto query = sql.str();
  mdlog::info("describe_table: " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error(result->GetError());
  }

  for (const auto &row : result->Collection().GetRows()) {
    columns.push_back(
        column_def{row.GetValue(0).GetValue<duckdb::string>(),
                   duckdb::EnumUtil::FromString<duckdb::LogicalTypeId>(
                       row.GetValue(1).GetValue<duckdb::string>()),
                   row.GetValue(2).GetValue<bool>()});
  }
  return columns;
}

void alter_table(duckdb::Connection &con, const std::string &db_name,
                 const std::string &schema_name, const std::string &table_name,
                 const std::vector<column_def> &columns) {

  std::set<std::string> alter_types;
  std::set<std::string> added_columns;
  std::set<std::string> deleted_columns;

  const auto &existing_columns =
      describe_table(con, db_name, schema_name, table_name);
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
    out << "ALTER TABLE " +
               compute_absolute_table_name(db_name, schema_name, table_name)
        << " ADD COLUMN ";
    const auto &col = new_column_map[col_name];

    out << KeywordHelper::WriteQuoted(col_name, '"') << " "
        << duckdb::EnumUtil::ToChars(col.type);
    if (col.primary_key) {
      out << " PRIMARY KEY";
    }
    auto query = out.str();
    mdlog::info("alter_table: " + query);

    auto result = con.Query(query);
    if (result->HasError()) {
      throw std::runtime_error(result->GetError());
    }
  }

  for (const auto &col_name : deleted_columns) {
    std::ostringstream out;
    out << "ALTER TABLE "
        << compute_absolute_table_name(db_name, schema_name, table_name)
        << " DROP COLUMN ";

    out << KeywordHelper::WriteQuoted(col_name, '"');

    auto query = out.str();
    mdlog::info("alter_table: " + query);
    auto result = con.Query(query);
    if (result->HasError()) {
      throw std::runtime_error(result->GetError());
    }
  }

  for (const auto &col_name : alter_types) {
    std::ostringstream out;
    out << "ALTER TABLE "
        << compute_absolute_table_name(db_name, schema_name, table_name)
        << " ALTER ";
    const auto &col = new_column_map[col_name];

    out << KeywordHelper::WriteQuoted(col_name, '"') << " TYPE "
        << duckdb::EnumUtil::ToChars(col.type);

    auto query = out.str();
    auto result = con.Query(query);
    if (result->HasError()) {
      throw std::runtime_error(result->GetError());
    }
  }
}

void upsert(duckdb::Connection &con, const std::string &db_name,
            const std::string &schema_name, const std::string &table_name,
            const std::string &staging_table_name,
            const std::vector<std::string> &primary_keys,
            const std::vector<column_def> &columns) {
  std::ostringstream sql;
  sql << "INSERT INTO "
      << compute_absolute_table_name(db_name, schema_name, table_name)
      << " SELECT * EXCLUDE (_fivetran_deleted, _fivetran_synced) FROM "
      << staging_table_name;
  if (!primary_keys.empty()) {
    sql << " ON CONFLICT (";
    write_joined(
        sql, primary_keys,
        [](const std::string &str, std::ostringstream &out) { out << str; });
    sql << " ) DO UPDATE SET ";

    write_joined(
        sql, columns, [](const column_def &col, std::ostringstream &out) {
          out << KeywordHelper::WriteQuoted(col.name, '"') << " = "
              << "excluded." << KeywordHelper::WriteQuoted(col.name, '"');
        });
  }

  auto query = sql.str();
  mdlog::info("upsert: " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error(result->GetError());
  }
}

void update_values(duckdb::Connection &con, const std::string &db_name,
                   const std::string &schema_name,
                   const std::string &table_name,
                   const std::string &staging_table_name,
                   const std::vector<std::string> &primary_keys,
                   const std::vector<column_def> &columns,
                   const std::string &unmodified_string) {

  std::ostringstream sql;
  auto absolute_table_name =
      compute_absolute_table_name(db_name, schema_name, table_name);

  sql << "UPDATE " << absolute_table_name << " SET ";

  write_joined(sql, columns,
               [staging_table_name, absolute_table_name, unmodified_string](
                   const column_def &col, std::ostringstream &out) {
                 auto colname = KeywordHelper::WriteQuoted(col.name, '"');
                 out << colname << " = CASE WHEN " << staging_table_name << "."
                     << colname << " = "
                     << KeywordHelper::WriteQuoted(unmodified_string, '\'')
                     << " THEN " << absolute_table_name << "." << colname
                     << " ELSE " << staging_table_name << "." << colname
                     << " END";
               });

  sql << " FROM " << staging_table_name << " WHERE ";
  write_joined(sql, primary_keys,
               [table_name, staging_table_name](const std::string &pk,
                                                std::ostringstream &out) {
                 out << table_name << "." << KeywordHelper::WriteQuoted(pk, '"')
                     << " = " << staging_table_name << "."
                     << KeywordHelper::WriteQuoted(pk, '"');
               });

  auto query = sql.str();
  mdlog::info("update: " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error(result->GetError());
  }
}

void delete_rows(duckdb::Connection &con, const std::string &db_name,
                 const std::string &schema_name, const std::string &table_name,
                 const std::string &staging_table_name,
                 const std::vector<std::string> &primary_keys) {
  std::ostringstream sql;
  sql << "DELETE FROM " +
             compute_absolute_table_name(db_name, schema_name, table_name)
      << " USING " << staging_table_name << " WHERE ";

  write_joined(sql, primary_keys,
               [table_name, staging_table_name](const std::string &pk,
                                                std::ostringstream &out) {
                 out << table_name << "." << KeywordHelper::WriteQuoted(pk, '"')
                     << " = " << staging_table_name << "."
                     << KeywordHelper::WriteQuoted(pk, '"');
               });

  auto query = sql.str();
  mdlog::info("delete_rows: " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error(result->GetError());
  }
}

void truncate_table(duckdb::Connection &con, const std::string &db_name,
                    const std::string &schema_name,
                    const std::string &table_name) {
  std::ostringstream sql;
  sql << "DELETE FROM " +
             compute_absolute_table_name(db_name, schema_name, table_name);
  auto query = sql.str();
  mdlog::info("truncate_table: " + query);
  auto result = con.Query(query);
  if (result->HasError()) {
    throw std::runtime_error(result->GetError());
  }
}

void check_connection(duckdb::Connection &con) {
  auto result = con.Query("SELECT 1");
  if (result->HasError()) {
    throw std::runtime_error(result->GetError());
  }
}