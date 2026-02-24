#include "sql_generator.hpp"

#include "duckdb.hpp"
#include "fivetran_duckdb_interop.hpp"
#include "md_error.hpp"
#include "md_logging.hpp"
#include "schema_types.hpp"

#include <chrono>
#include <cstdint>
#include <fmt/format.h>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using duckdb::KeywordHelper;

void find_primary_keys(const std::vector<column_def>& cols, std::vector<const column_def*>& columns_pk,
                       std::vector<const column_def*>* columns_regular, const std::string& ignored_primary_key) {
	for (auto& col : cols) {
		if (col.primary_key && col.name != ignored_primary_key) {
			columns_pk.push_back(&col);
		} else if (columns_regular != nullptr) {
			columns_regular->push_back(&col);
		}
	}
}

struct TransactionContext {
	explicit TransactionContext(duckdb::Connection& con_) : con(con_) {
		auto should_begin = !con.HasActiveTransaction();
		if (should_begin) {
			con.BeginTransaction();
		}
		has_begun = should_begin;
	}

	void Commit() {
		if (has_begun) {
			con.Commit();
			has_begun = false;
		}
	}

	~TransactionContext() {
		// We should commit the context before it goes out of scope. When this
		// doesn't happen, HasActiveTransaction() is true. However, if the context
		// did not begin a new transaction because the connection already had an
		// active transaction from an outer scope (i.e. should_begin, and therefore
		// has_begun are false), we don't want to rollback because it is expected
		// that the outer transaction should remain active.
		if (con.HasActiveTransaction() && has_begun) {
			con.Rollback();
		}
	}

private:
	duckdb::Connection& con;
	bool has_begun;
};

namespace {
// Utility

const auto print_column = [](const std::string& quoted_col, std::ostringstream& out) {
	out << quoted_col;
};

void write_joined(std::ostringstream& sql, const std::vector<const column_def*>& columns,
                  std::function<void(const std::string&, std::ostringstream&)> print_str,
                  const std::string& separator = ", ") {
	bool first = true;
	for (const auto& col : columns) {
		if (first) {
			first = false;
		} else {
			sql << separator;
		}
		print_str(KeywordHelper::WriteQuoted(col->name, '"'), sql);
	}
}

std::string make_full_column_list(const std::vector<const column_def*>& columns_pk,
                                  const std::vector<const column_def*>& columns_regular) {
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

std::string primary_key_join(std::vector<const column_def*>& columns_pk, const std::string tbl1,
                             const std::string tbl2) {
	std::ostringstream primary_key_join_condition_stream;
	write_joined(
	    primary_key_join_condition_stream, columns_pk,
	    [&](const std::string& quoted_col, std::ostringstream& out) {
		    out << tbl1 << "." << quoted_col << " = " << tbl2 << "." << quoted_col;
	    },
	    " AND ");
	return primary_key_join_condition_stream.str();
}
} // namespace

MdSqlGenerator::MdSqlGenerator(mdlog::Logger& logger_) : logger(logger_) {
}

std::string MdSqlGenerator::generate_temp_table_name(duckdb::Connection& con, const std::string& prefix) const {
	const auto current_db_res = con.Query("SELECT current_database()");
	if (current_db_res->HasError()) {
		current_db_res->ThrowError("Could not get current database to generate temporary table name: ");
	}
	assert(current_db_res->RowCount() == 1);
	assert(current_db_res->ColumnCount() == 1);
	const std::string current_db = current_db_res->GetValue(0, 0).ToString();
	const std::string current_path = KeywordHelper::WriteQuoted(current_db, '"') + ".\"main\"";

	constexpr uint_fast8_t MAX_ATTEMPTS = 10; // This should be more than enough
	for (uint_fast8_t i = 0; i < MAX_ATTEMPTS; i++) {
		const std::string table_name = prefix + duckdb::StringUtil::GenerateRandomName(16);
		const std::string fqn_name = current_path + "." + KeywordHelper::WriteQuoted(table_name, '"');
		const std::string check_query =
		    "FROM (SHOW TABLES FROM " + current_path + ") WHERE name = " + KeywordHelper::WriteQuoted(table_name, '\'');
		const auto check_res = con.Query(check_query);
		if (check_res->HasError()) {
			logger.severe("Could not check for existence of temporary table <" + table_name +
			              ">: " + check_res->GetError());
			// Optimistically use this name in case there was an error during checking
			return fqn_name;
		}

		// If there is no such table, we can use this name
		if (check_res->RowCount() == 0) {
			return fqn_name;
		}
	}

	throw std::runtime_error("Could not generate a unique temporary table name after " + std::to_string(MAX_ATTEMPTS) +
	                         " attempts");
}

void MdSqlGenerator::run_query(duckdb::Connection& con, const std::string& log_prefix, const std::string& query,
                               const std::string& error_message) const {
	logger.info(log_prefix + ": " + query);
	const auto result = con.Query(query);
	if (result->HasError()) {
		throw std::runtime_error(error_message + ": " + result->GetError());
	}
}

bool MdSqlGenerator::table_exists(duckdb::Connection& con, const table_def& table) {
	const std::string query = "SELECT table_name FROM information_schema.tables WHERE "
	                          "table_catalog=? AND table_schema=? AND table_name=?";
	const std::string err = "Could not find whether table <" + table.to_escaped_string() + "> exists";
	auto statement = con.Prepare(query);
	logger.info("    prepared table_exists query for table " + table.table_name);
	if (statement->HasError()) {
		throw std::runtime_error(err + " (at bind step): " + statement->GetError());
	}
	duckdb::vector<duckdb::Value> params = {duckdb::Value(table.db_name), duckdb::Value(table.schema_name),
	                                        duckdb::Value(table.table_name)};
	auto result = statement->Execute(params, false);
	logger.info("    executed table_exists query for table " + table.table_name);

	if (result->HasError()) {
		throw std::runtime_error(err + ": " + result->GetError());
	}
	auto materialized_result =
	    duckdb::unique_ptr_cast<duckdb::QueryResult, duckdb::MaterializedQueryResult>(std::move(result));
	logger.info("    materialized table_exists results for table " + table.table_name);
	return materialized_result->RowCount() > 0;
}

namespace {
duckdb::unique_ptr<duckdb::MaterializedQueryResult> create_schema_if_not_exists(duckdb::Connection& con,
                                                                                const std::string& db_name,
                                                                                const std::string& schema_name,
                                                                                mdlog::Logger& logger) {
	std::ostringstream ddl;
	ddl << "CREATE SCHEMA IF NOT EXISTS " << KeywordHelper::WriteQuoted(db_name, '"') << "."
	    << KeywordHelper::WriteQuoted(schema_name, '"');
	const std::string query = ddl.str();
	logger.info("create_schema_if_not_exists: " + query);
	return con.Query(query);
}

/// Retries the given idempotent operation after a short delay if it fails due to a transaction write-write conflict.
/// @param operation An idempotent operation to execute and potentially retry if it fails due to a write-write conflict.
/// @param max_retries The maximum number of retries before giving up and returning the last error result.
duckdb::unique_ptr<duckdb::MaterializedQueryResult>
retry_transaction_errors(const std::function<duckdb::unique_ptr<duckdb::MaterializedQueryResult>()>& operation,
                         const uint_fast8_t max_retries = 8) {
	uint_fast8_t attempt = 0;
	duckdb::unique_ptr<duckdb::MaterializedQueryResult> result = operation();

	while (result->HasError() && attempt < max_retries) {
		const auto& error_data = result->GetErrorObject();
		// We only retry transaction conflicts
		if (error_data.Type() != duckdb::ExceptionType::TRANSACTION ||
		    error_data.RawMessage().find("Catalog write-write conflict") == std::string::npos) {
			break;
		}

		// Since function has been built with `CREATE SCHEMA IF NOT EXISTS` queries in mind.
		// The assumption here is that we have a short queue of connections doing short-lived transactions on the same
		// catalog object, and that this queue is not growing. We expect one at least one transaction to be successful
		// per round/attempt, hence we retry maximum 8 times (number of parallel threads in Fivetran). We add a bit of
		// jitter to reduce the chance of conflicts and therefore retries.
		thread_local std::mt19937 gen(std::random_device {}());
		// It is fine to retry immediately (i.e. 0 ms delay), but in the common case, we wait for a short amount of
		// time.
		std::uniform_int_distribution<std::uint_fast8_t> dis(0, 100);
		const auto delay_ms = dis(gen);
		std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

		result = operation();
		attempt++;
	}

	return result;
}

} // namespace

void MdSqlGenerator::create_schema_if_not_exists_with_retries(duckdb::Connection& con, const std::string& db_name,
                                                              const std::string& schema_name) const {
	const auto create_result =
	    retry_transaction_errors([&]() { return create_schema_if_not_exists(con, db_name, schema_name, logger); });

	if (create_result->HasError()) {
		throw std::runtime_error("Could not create schema <" + schema_name + "> in database <" + db_name +
		                         ">: " + create_result->GetError());
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

void MdSqlGenerator::create_table(duckdb::Connection& con, const table_def& table,
                                  const std::vector<column_def>& all_columns,
                                  const std::set<std::string>& columns_with_default_value) {
	const std::string absolute_table_name = table.to_escaped_string();

	std::vector<const column_def*> columns_pk;
	find_primary_keys(all_columns, columns_pk);

	std::ostringstream ddl;
	ddl << "CREATE OR REPLACE TABLE " << absolute_table_name << " (";

	for (const auto& col : all_columns) {
		ddl << KeywordHelper::WriteQuoted(col.name, '"') << " " << format_type(col);
		if (columns_with_default_value.find(col.name) != columns_with_default_value.end()) {
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
	logger.info("create_table: " + query);

	const auto result = con.Query(query);
	if (result->HasError()) {
		const std::string& error_msg = result->GetError();

		if (error_msg.find("is attached in read-only mode") != std::string::npos) {
			throw md_error::RecoverableError("The database is attached in read-only mode. Please make sure your "
			                                 "MotherDuck token is a Read/Write "
			                                 "Token and check that it can write to the target database.");
		}

		throw std::runtime_error("Could not create table <" + absolute_table_name + ">: " + error_msg);
	}
}

std::vector<column_def> MdSqlGenerator::describe_table(duckdb::Connection& con, const table_def& table) {
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
	const std::string err = "Could not describe table <" + table.to_escaped_string() + ">";
	logger.info("describe_table: " + std::string(query));
	auto statement = con.Prepare(query);
	if (statement->HasError()) {
		throw std::runtime_error(err + " (at bind step): " + statement->GetError());
	}
	duckdb::vector<duckdb::Value> params = {duckdb::Value(table.db_name), duckdb::Value(table.schema_name),
	                                        duckdb::Value(table.table_name)};
	auto result = statement->Execute(params, false);

	if (result->HasError()) {
		throw std::runtime_error(err + ": " + result->GetError());
	}

	auto& materialized_result = result->Cast<duckdb::MaterializedQueryResult>();

	for (const auto& row : materialized_result.Collection().GetRows()) {
		duckdb::LogicalTypeId column_type = static_cast<duckdb::LogicalTypeId>(row.GetValue(1).GetValue<int8_t>());
		column_def col {
		    row.GetValue(0).GetValue<duckdb::string>(), column_type, row.GetValue(2).GetValue<duckdb::string>(),
		    row.GetValue(3).GetValue<bool>(),           0,           0};
		if (column_type == duckdb::LogicalTypeId::DECIMAL) {
			col.width = row.GetValue(4).GetValue<uint8_t>();
			col.scale = row.GetValue(5).GetValue<uint8_t>();
		}
		columns.push_back(col);
	}
	return columns;
}

void MdSqlGenerator::add_column(duckdb::Connection& con, const table_def& table, const column_def& column,
                                const std::string& log_prefix, const bool ignore_if_exists) const {
	// Add `column` to `table` and add a default value if present in the struct.
	std::ostringstream sql;
	sql << "ALTER TABLE " << table.to_escaped_string() << " ADD COLUMN ";

	if (ignore_if_exists) {
		sql << " IF NOT EXISTS ";
	}

	sql << KeywordHelper::WriteQuoted(column.name, '"') << " " << format_type(column);

	if (column.column_default.has_value()) {
		if (column.column_default.value() == "NULL") {
			logger.info("Detected string \"NULL\" as default value for column " + column.name);
		}
		// We should not expect NULLs here according to fivetran, so we also cast
		// the string "NULL" to the string "NULL" for varchar columns, not NULLs.
		sql << " DEFAULT CAST(" << KeywordHelper::WriteQuoted(column.column_default.value(), '\'') << " AS "
		    << format_type(column) << ")";
	}

	return run_query(con, log_prefix, sql.str(),
	                 "Could not add column <" + column.name + "> to table <" + table.to_escaped_string() + ">");
}

void MdSqlGenerator::drop_column(duckdb::Connection& con, const table_def& table, const std::string& column_name,
                                 const std::string& log_prefix, const bool not_exists_ok) const {
	std::ostringstream sql;
	sql << "ALTER TABLE " << table.to_escaped_string() << " DROP COLUMN ";

	if (not_exists_ok) {
		sql << " IF EXISTS ";
	}
	sql << KeywordHelper::WriteQuoted(column_name, '"');

	return run_query(con, log_prefix, sql.str(),
	                 "Could not drop column <" + column_name + "> of table <" + table.to_escaped_string() + ">");
}

void MdSqlGenerator::alter_table_recreate(duckdb::Connection& con, const table_def& table,
                                          const std::vector<column_def>& all_columns,
                                          const std::set<std::string>& common_columns) {
	long timestamp =
	    std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	auto temp_table =
	    table_def {table.db_name, table.schema_name, "tmp_" + table.table_name + "_" + std::to_string(timestamp)};
	auto absolute_table_name = table.to_escaped_string();
	auto absolute_temp_table_name = temp_table.to_escaped_string();

	rename_table(con, table, temp_table.table_name, "alter_table_recreate rename");

	// new primary keys have to get a default value as they cannot be null
	std::set<std::string> new_primary_key_cols;
	for (const auto& col : all_columns) {
		if (col.primary_key && common_columns.find(col.name) == common_columns.end()) {
			new_primary_key_cols.insert(col.name);
		}
	}

	create_table(con, table, all_columns, new_primary_key_cols);

	// reinsert the data from the old table
	std::ostringstream out_column_list;
	bool first = true;
	for (auto& col : common_columns) {
		if (first) {
			first = false;
		} else {
			out_column_list << ",";
		}
		out_column_list << KeywordHelper::WriteQuoted(col, '"');
	}
	std::string common_column_list = out_column_list.str();

	std::ostringstream out;
	out << "INSERT INTO " << absolute_table_name << "(" << common_column_list << ") SELECT " << common_column_list
	    << " FROM " << absolute_temp_table_name;

	run_query(con, "Reinserting data after changing primary keys", out.str(),
	          "Could not reinsert data into table <" + absolute_table_name + ">");
	drop_table(con, temp_table, "alter_table_recreate drop");
}

void MdSqlGenerator::alter_table_in_place(duckdb::Connection& con, const table_def& table,
                                          const std::vector<column_def>& added_columns,
                                          const std::set<std::string>& deleted_columns,
                                          const std::set<std::string>& alter_types,
                                          const std::map<std::string, column_def>& new_column_map) {
	for (const auto& col : added_columns) {
		add_column(con, table, col, "alter_table add");
	}

	auto absolute_table_name = table.to_escaped_string();

	for (const auto& col_name : alter_types) {
		std::ostringstream out;
		out << "ALTER TABLE " << absolute_table_name << " ALTER ";
		const auto& col = new_column_map.at(col_name);

		out << KeywordHelper::WriteQuoted(col_name, '"') << " TYPE " << format_type(col);

		run_query(con, "alter table change type", out.str(),
		          "Could not alter type for column <" + col_name + "> in table <" + absolute_table_name + ">");
	}

	for (const auto& col_name : deleted_columns) {
		drop_column(con, table, col_name, "alter_table drop", false);
	}
}

void MdSqlGenerator::alter_table(duckdb::Connection& con, const table_def& table,
                                 const std::vector<column_def>& requested_columns, const bool drop_columns) {
	bool recreate_table = false;

	auto absolute_table_name = table.to_escaped_string();
	std::set<std::string> alter_types;
	std::set<std::string> added_columns;
	std::set<std::string> deleted_columns;
	std::set<std::string> common_columns;

	logger.info("    in MdSqlGenerator::alter_table for " + absolute_table_name);
	const auto& existing_columns = describe_table(con, table);
	std::map<std::string, column_def> new_column_map;

	// start by assuming all columns are new
	for (const auto& col : requested_columns) {
		new_column_map.emplace(col.name, col);
		added_columns.emplace(col.name);
	}

	// make added_columns correct by removing previously existing columns
	for (const auto& col : existing_columns) {
		const auto& new_col_it = new_column_map.find(col.name);

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
				logger.info("Source connector requested that table " + absolute_table_name + " column " + col.name +
				            " be dropped, but dropping columns is not allowed when "
				            "drop_columns is false");
			}
		} else if (new_col_it->second.primary_key != col.primary_key) {
			logger.info("Altering primary key requested for column <" + new_col_it->second.name + ">");
			recreate_table = true;
		} else if (new_col_it->second.type != col.type ||
		           (new_col_it->second.type == duckdb::LogicalTypeId::DECIMAL &&
		            (new_col_it->second.scale != col.scale || new_col_it->second.width != col.width))) {
			alter_types.emplace(col.name);
		}
	}
	logger.info("    inventoried columns; recreate_table = " + std::to_string(recreate_table) +
	            "; num alter_types = " + std::to_string(alter_types.size()));

	auto primary_key_added_it =
	    std::find_if(added_columns.begin(), added_columns.end(), [&new_column_map](const std::string& column_name) {
		    return new_column_map[column_name].primary_key;
	    });
	if (primary_key_added_it != added_columns.end()) {
		logger.info("Adding primary key requested for column <" + *primary_key_added_it + ">");
		recreate_table = true;
	}

	// list added columns in order
	std::vector<column_def> added_columns_ordered;
	for (const auto& col : requested_columns) {
		const auto& new_col_it = added_columns.find(col.name);
		if (new_col_it != added_columns.end()) {
			added_columns_ordered.push_back(new_column_map[col.name]);
			logger.info("    adding column " + col.name);
		}
	}

	TransactionContext transaction_context(con);

	if (recreate_table) {
		logger.info("    recreating table");
		// preserve the order of the original columns
		auto all_columns = existing_columns;

		// replace definitions of existing columns with the new ones if available
		for (size_t i = 0; i < all_columns.size(); i++) {
			const auto& new_col_it = new_column_map.find(all_columns[i].name);
			if (new_col_it != new_column_map.end()) {
				all_columns[i] = new_col_it->second;
			}
		}

		// add new columns to the end of the table, in order they appear in the
		// request
		for (const auto& col : added_columns_ordered) {
			all_columns.push_back(col);
		}
		alter_table_recreate(con, table, all_columns, common_columns);
	} else {
		logger.info("    altering table in place");
		alter_table_in_place(con, table, added_columns_ordered, deleted_columns, alter_types, new_column_map);
	}

	transaction_context.Commit();
}

void MdSqlGenerator::upsert(duckdb::Connection& con, const table_def& table, const std::string& staging_table_name,
                            const std::vector<const column_def*>& columns_pk,
                            const std::vector<const column_def*>& columns_regular) {

	auto full_column_list = make_full_column_list(columns_pk, columns_regular);
	const std::string absolute_table_name = table.to_escaped_string();
	std::ostringstream sql;
	sql << "INSERT INTO " << absolute_table_name << "(" << full_column_list << ") SELECT " << full_column_list
	    << " FROM " << staging_table_name;

	if (!columns_pk.empty()) {
		sql << " ON CONFLICT (";
		write_joined(sql, columns_pk, print_column);
		sql << " ) DO UPDATE SET ";

		write_joined(sql, columns_regular, [](const std::string& quoted_col, std::ostringstream& out) {
			out << quoted_col << " = excluded." << quoted_col;
		});
	}

	auto query = sql.str();
	logger.info("upsert: " + query);
	auto result = con.Query(query);
	if (result->HasError()) {
		throw std::runtime_error("Could not upsert table <" + absolute_table_name + ">" + result->GetError());
	}
}

void MdSqlGenerator::insert(duckdb::Connection& con, const table_def& table, const std::string& staging_table_name,
                            const std::vector<const column_def*>& columns_pk,
                            const std::vector<const column_def*>& columns_regular) {

	auto full_column_list = make_full_column_list(columns_pk, columns_regular);
	const std::string absolute_table_name = table.to_escaped_string();
	std::ostringstream sql;
	sql << "INSERT INTO " << absolute_table_name << "(" << full_column_list << ") SELECT " << full_column_list
	    << " FROM " << staging_table_name;

	auto query = sql.str();
	logger.info("insert: " + query);
	auto result = con.Query(query);
	if (result->HasError()) {
		throw std::runtime_error("Could not insert into table <" + absolute_table_name + ">" + result->GetError());
	}
}

void MdSqlGenerator::update_values(duckdb::Connection& con, const table_def& table,
                                   const std::string& staging_table_name, std::vector<const column_def*>& columns_pk,
                                   std::vector<const column_def*>& columns_regular,
                                   const std::string& unmodified_string) {

	logger.info("MdSqlGenerator::update_values requested");
	std::ostringstream sql;
	auto absolute_table_name = table.to_escaped_string();

	sql << "UPDATE " << absolute_table_name << " SET ";

	write_joined(sql, columns_regular,
	             [staging_table_name, absolute_table_name, unmodified_string](const std::string quoted_col,
	                                                                          std::ostringstream& out) {
		             out << quoted_col << " = CASE WHEN " << staging_table_name << "." << quoted_col << " = "
		                 << KeywordHelper::WriteQuoted(unmodified_string, '\'') << " THEN " << absolute_table_name
		                 << "." << quoted_col << " ELSE " << staging_table_name << "." << quoted_col << " END";
	             });

	sql << " FROM " << staging_table_name << " WHERE ";
	write_joined(
	    sql, columns_pk,
	    [&](const std::string& quoted_col, std::ostringstream& out) {
		    out << KeywordHelper::WriteQuoted(table.table_name, '"') << "." << quoted_col << " = " << staging_table_name
		        << "." << quoted_col;
	    },
	    " AND ");

	auto query = sql.str();
	logger.info("update: " + query);
	auto result = con.Query(query);
	if (result->HasError()) {
		throw std::runtime_error("Could not update table <" + absolute_table_name + ">:" + result->GetError());
	}
}

std::string MdSqlGenerator::create_latest_active_records_table(duckdb::Connection& con,
                                                               const table_def& source_table) const {
	const std::string lar_table_name = generate_temp_table_name(con, "__fivetran_latest_active_records");
	const auto create_lar_table_res =
	    con.Query("CREATE TABLE " + lar_table_name + " AS FROM " + source_table.to_escaped_string() + " WITH NO DATA");
	if (create_lar_table_res->HasError()) {
		create_lar_table_res->ThrowError("Could not create latest_active_records table: ");
	}
	return lar_table_name;
}

void MdSqlGenerator::drop_latest_active_records_table(duckdb::Connection& con,
                                                      const std::string& lar_table_name) const {
	const auto drop_lar_table_res = con.Query("DROP TABLE IF EXISTS " + lar_table_name);
	if (drop_lar_table_res->HasError()) {
		// Log error, but do not throw. In the worst case, this leaves a
		// table lingering.
		logger.severe("Could not drop latest_active_records table " + lar_table_name + ": " +
		              drop_lar_table_res->GetError());
	}
}

void MdSqlGenerator::add_partial_historical_values(duckdb::Connection& con, const table_def& table,
                                                   const std::string& staging_table_name,
                                                   const std::string& lar_table_name,
                                                   std::vector<const column_def*>& columns_pk,
                                                   std::vector<const column_def*>& columns_regular,
                                                   const std::string& unmodified_string) const {
	std::ostringstream sql;
	auto absolute_table_name = table.to_escaped_string();

	auto full_column_list = make_full_column_list(columns_pk, columns_regular);
	sql << "INSERT INTO " << absolute_table_name << " (" << full_column_list << ") ( SELECT ";

	// use primary keys as is, without checking for unmodified value
	write_joined(
	    sql, columns_pk,
	    [&](const std::string& quoted_col, std::ostringstream& out) { out << staging_table_name << "." << quoted_col; },
	    ", ");
	sql << ",  ";

	write_joined(sql, columns_regular,
	             [staging_table_name, unmodified_string](const std::string quoted_col, std::ostringstream& out) {
		             out << "CASE WHEN " << staging_table_name << "." << quoted_col << " = "
		                 << KeywordHelper::WriteQuoted(unmodified_string, '\'') << " THEN lar." << quoted_col
		                 << " ELSE " << staging_table_name << "." << quoted_col << " END as " << quoted_col;
	             });

	sql << " FROM " << staging_table_name << " LEFT JOIN " << lar_table_name << " AS lar ON "
	    << primary_key_join(columns_pk, "lar", staging_table_name) << ")";

	auto query = sql.str();
	logger.info("update (add partial historical values): " + query);
	auto result = con.Query(query);
	if (result->HasError()) {
		throw std::runtime_error("Could not update (add partial historical values) table <" + absolute_table_name +
		                         ">:" + result->GetError());
	}
}

void MdSqlGenerator::delete_rows(duckdb::Connection& con, const table_def& table, const std::string& staging_table_name,
                                 std::vector<const column_def*>& columns_pk) {

	const std::string absolute_table_name = table.to_escaped_string();
	std::ostringstream sql;
	sql << "DELETE FROM " + absolute_table_name << " USING " << staging_table_name << " WHERE ";

	write_joined(
	    sql, columns_pk,
	    [&](const std::string& quoted_col, std::ostringstream& out) {
		    out << KeywordHelper::WriteQuoted(table.table_name, '"') << "." << quoted_col << " = " << staging_table_name
		        << "." << quoted_col;
	    },
	    " AND ");

	auto query = sql.str();
	logger.info("delete_rows: " + query);
	auto result = con.Query(query);
	if (result->HasError()) {
		throw std::runtime_error("Error deleting rows from table <" + absolute_table_name + ">:" + result->GetError());
	}
}

void MdSqlGenerator::deactivate_historical_records(duckdb::Connection& con, const table_def& table,
                                                   const std::string& staging_table_name,
                                                   const std::string& lar_table_name,
                                                   std::vector<const column_def*>& columns_pk) const {

	const std::string absolute_table_name = table.to_escaped_string();

	// primary keys condition (list of primary keys already excludes
	// _fivetran_start)
	std::string primary_key_join_condition = primary_key_join(columns_pk, absolute_table_name, staging_table_name);

	{
		// delete overlapping records
		std::ostringstream sql;
		sql << "DELETE FROM " + absolute_table_name << " USING " << staging_table_name << " WHERE ";
		sql << primary_key_join_condition;
		sql << " AND " << absolute_table_name << "._fivetran_start >= " << staging_table_name << "._fivetran_start";

		auto query = sql.str();
		logger.info("delete_overlapping_records: " + query);
		auto result = con.Query(query);
		if (result->HasError()) {
			throw std::runtime_error("Error deleting overlapping records from table <" + absolute_table_name +
			                         ">:" + result->GetError());
		}
	}

	{
		// store latest versions of records before they get deactivated
		// per spec, this should be limited to _fivetran_active = TRUE, but it's
		// safer to get all latest versions even if deactivated to prevent null
		// values in a partially successful batch.
		std::ostringstream sql;
		const std::string short_table_name = KeywordHelper::WriteQuoted(table.table_name, '"');
		sql << "WITH ranked_records AS (SELECT " << short_table_name << ".*,";
		sql << " row_number() OVER (PARTITION BY ";
		write_joined(sql, columns_pk, [&](const std::string& quoted_col, std::ostringstream& out) {
			out << absolute_table_name << "." << quoted_col;
		});
		sql << " ORDER BY " << absolute_table_name << "._fivetran_start DESC) as row_num FROM " << absolute_table_name;
		// inner join to earliest table to only select rows that are in this batch
		sql << " INNER JOIN " << staging_table_name << " ON " << primary_key_join_condition << ")\n";

		sql << "INSERT INTO " << lar_table_name
		    << " SELECT * EXCLUDE (row_num) FROM "
		       "ranked_records WHERE row_num = 1";
		auto query = sql.str();
		logger.info("stash latest records: " + query);
		auto result = con.Query(query);
		if (result->HasError()) {
			throw std::runtime_error("Error stashing latest records from table <" + absolute_table_name +
			                         ">:" + result->GetError());
		}
	}

	{
		// mark existing records inactive
		std::ostringstream sql;
		sql << "UPDATE " + absolute_table_name << " SET _fivetran_active = FALSE, ";
		// converting to TIMESTAMP with no timezone because otherwise ICU is
		// required to do TIMESTAMPZ math. Need to test this well.
		sql << "_fivetran_end = (" << staging_table_name << "._fivetran_start::TIMESTAMP - (INTERVAL '1 millisecond'))";
		sql << " FROM " << staging_table_name;
		sql << " WHERE " << absolute_table_name << "._fivetran_active = TRUE AND ";
		sql << primary_key_join_condition;

		auto query = sql.str();
		logger.info("deactivate records: " + query);
		auto result = con.Query(query);
		if (result->HasError()) {
			throw std::runtime_error("Error deactivating records <" + absolute_table_name + ">:" + result->GetError());
		}
	}
}

void MdSqlGenerator::delete_historical_rows(duckdb::Connection& con, const table_def& table,
                                            const std::string& staging_table_name,
                                            std::vector<const column_def*>& columns_pk) {

	const std::string absolute_table_name = table.to_escaped_string();

	std::string primary_key_join_condition = primary_key_join(columns_pk, absolute_table_name, staging_table_name);

	std::ostringstream sql;

	sql << "UPDATE " + absolute_table_name << " SET _fivetran_active = FALSE, ";
	sql << "_fivetran_end = " << staging_table_name << "._fivetran_end";
	sql << " FROM " << staging_table_name;
	sql << " WHERE " << absolute_table_name << "._fivetran_active = TRUE AND ";
	sql << primary_key_join_condition;

	auto query = sql.str();
	logger.info("delete historical records: " + query);
	auto result = con.Query(query);
	if (result->HasError()) {
		throw std::runtime_error("Error deleting historical records <" + absolute_table_name +
		                         ">:" + result->GetError());
	}
}

void MdSqlGenerator::truncate_table(duckdb::Connection& con, const table_def& table, const std::string& synced_column,
                                    std::chrono::nanoseconds& cutoff_ns, const std::string& deleted_column) {
	const std::string absolute_table_name = table.to_escaped_string();
	std::ostringstream sql;

	logger.info("truncate_table request: deleted column = " + deleted_column);
	if (deleted_column.empty()) {
		// hard delete
		sql << "DELETE FROM " << absolute_table_name;
	} else {
		// soft delete
		sql << "UPDATE " << absolute_table_name << " SET " << KeywordHelper::WriteQuoted(deleted_column, '"')
		    << " = true";
	}
	logger.info("truncate_table request: synced column = " + synced_column);
	sql << " WHERE " << KeywordHelper::WriteQuoted(synced_column, '"') << " < make_timestamp(?)";
	auto query = sql.str();
	const std::string err = "Error truncating table at bind step <" + absolute_table_name + ">";
	logger.info("truncate_table: " + query);
	auto statement = con.Prepare(query);
	if (statement->HasError()) {
		throw std::runtime_error(err + " (at bind step):" + statement->GetError());
	}

	// DuckDB make_timestamp takes microseconds; Fivetran sends millisecond
	// precision -- safe to divide with truncation
	int64_t cutoff_microseconds = cutoff_ns.count() / 1000;
	duckdb::vector<duckdb::Value> params = {duckdb::Value(cutoff_microseconds)};

	logger.info("truncate_table: cutoff_microseconds = <" + std::to_string(cutoff_microseconds) + ">");
	auto result = statement->Execute(params, false);
	if (result->HasError()) {
		throw std::runtime_error(err + ": " + result->GetError());
	}
}

// Migration operations

void MdSqlGenerator::drop_table(duckdb::Connection& con, const table_def& table, const std::string& log_prefix) {
	const std::string absolute_table_name = table.to_escaped_string();

	run_query(con, log_prefix, "DROP TABLE " + absolute_table_name,
	          "Could not drop table <" + absolute_table_name + ">");
}

void MdSqlGenerator::drop_column_in_history_mode(duckdb::Connection& con, const table_def& table,
                                                 const std::string& column, const std::string& operation_timestamp) {
	const std::string absolute_table_name = table.to_escaped_string();
	const std::string quoted_column = KeywordHelper::WriteQuoted(column, '"');
	const std::string quoted_timestamp = KeywordHelper::WriteQuoted(operation_timestamp, '\'') + "::TIMESTAMPTZ";

	if (!history_table_is_valid(con, absolute_table_name, quoted_timestamp)) {
		// The table is empty
		return;
	}

	// Per spec: In history mode, dropping a column preserves historical data.
	// We execute 3 queries as described in the spec if the table is not empty.

	TransactionContext transaction_context(con);

	{
		// Query 1: Insert new rows for active records where column is not null
		std::ostringstream sql;
		sql << "INSERT INTO " << absolute_table_name << " SELECT * REPLACE"
		    << " (NULL as " << quoted_column << ", " << quoted_timestamp << " as \"_fivetran_start\""
		    << ")"
		    << " FROM " << absolute_table_name << " WHERE \"_fivetran_active\" = TRUE"
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
		sql << "UPDATE " << absolute_table_name << " SET \"_fivetran_active\" = FALSE,"
		    << " \"_fivetran_end\" = (" << quoted_timestamp << "::TIMESTAMP - (INTERVAL '1 millisecond'))"
		    << " WHERE \"_fivetran_active\" = TRUE"
		    << " AND " << quoted_column << " IS NOT NULL"
		    << " AND \"_fivetran_start\" < " << quoted_timestamp;

		run_query(con, "drop_column_in_history_mode update_prev", sql.str(),
		          "Could not update previous records for drop_column_in_history_mode");
	}

	transaction_context.Commit();
}

void MdSqlGenerator::copy_table(duckdb::Connection& con, const table_def& from_table, const table_def& to_table,
                                const std::string& log_prefix, const std::vector<const column_def*>& additional_pks) {
	TransactionContext transaction_context(con);

	{
		std::ostringstream sql;
		sql << "CREATE TABLE " << to_table.to_escaped_string() << " AS FROM " << from_table.to_escaped_string();

		run_query(con, log_prefix, sql.str(),
		          "Could not copy table <" + from_table.to_escaped_string() + "> to <" + to_table.to_escaped_string() +
		              ">");
	}

	const auto columns = describe_table(con, from_table);
	std::vector<const column_def*> columns_pk;
	std::vector<const column_def*> columns_regular;
	find_primary_keys(columns, columns_pk, &columns_regular, "_fivetran_start");

	// Merge primary key lists
	std::vector<const column_def*> combined_pks;
	combined_pks.reserve(columns_pk.size() + additional_pks.size());
	combined_pks.insert(combined_pks.end(), columns_pk.begin(), columns_pk.end());
	combined_pks.insert(combined_pks.end(), additional_pks.begin(), additional_pks.end());

	add_defaults(con, columns, to_table.to_escaped_string(), log_prefix);
	add_pks(con, combined_pks, to_table.to_escaped_string(), log_prefix);

	transaction_context.Commit();
}

void MdSqlGenerator::add_defaults(duckdb::Connection& con, const std::vector<column_def>& columns,
                                  const std::string& table_name, const std::string& log_prefix) {
	// Copies the default of every column that has a default defined to the destination table_name. This assumes all
	// columns are present in the destination table.
	for (const auto& col : columns) {
		// This also sets the default value of the soft_deleted_column if it was not
		// equal to _fivetran_deleted
		if (!col.column_default.has_value() || col.column_default.value() == "NULL") {
			continue;
		}

		std::ostringstream sql;
		sql << "ALTER TABLE " << table_name << " ALTER COLUMN " << KeywordHelper::WriteQuoted(col.name, '"')
		    << " SET DEFAULT " << KeywordHelper::WriteQuoted(col.column_default.value(), '\'') << ";";
		run_query(con, log_prefix, sql.str(), "Could not add default to column " + col.name);
	}
}

void MdSqlGenerator::add_pks(duckdb::Connection& con, const std::vector<const column_def*>& columns_pk,
                             const std::string& table_name, const std::string& log_prefix) const {
	if (columns_pk.empty()) {
		// All modes require a primary key to be present, because we cannot switch
		// to history mode without a primary key. Fivetran has confirmed that the
		// partner sdk assures existence of a primary key, and else adds a primary
		// key itself.
		throw std::runtime_error("No primary keys found for table " + table_name);
	}

	// Add the right primary key. Note that "CREATE TABLE AS SELECT" does not
	// add any primary key constraints.
	std::ostringstream sql;

	sql << "ALTER TABLE " << table_name << " ADD PRIMARY KEY (";
	write_joined(sql, columns_pk, print_column);
	sql << ");";
	run_query(con, log_prefix, sql.str(), "Could not add pks to table " + table_name);
}

void MdSqlGenerator::copy_column(duckdb::Connection& con, const table_def& table, const std::string& from_column,
                                 const std::string& to_name) {
	const std::string quoted_from = KeywordHelper::WriteQuoted(from_column, '"');
	const std::string quoted_to = KeywordHelper::WriteQuoted(to_name, '"');

	// Get the column type from the source column
	auto query = "SELECT data_type_id, column_default, numeric_precision, numeric_scale "
	             "from duckdb_columns() WHERE "
	             "database_name = " +
	             KeywordHelper::WriteQuoted(table.db_name, '\'') +
	             " AND schema_name = " + KeywordHelper::WriteQuoted(table.schema_name, '\'') +
	             " AND table_name = " + KeywordHelper::WriteQuoted(table.table_name, '\'') +
	             " AND column_name = " + KeywordHelper::WriteQuoted(from_column, '\'');
	auto result = con.Query(query);

	if (result->HasError()) {
		throw std::runtime_error("copy_column get_type: " + result->GetError());
	}
	if (result->RowCount() < 1) {
		throw std::runtime_error("Column with name " + quoted_from + " not found");
	}

	auto type = static_cast<duckdb::LogicalTypeId>(result->GetValue(0, 0).GetValue<int8_t>());

	column_def to_column {
	    .name = to_name,
	    .type = type,
	    .column_default = result->GetValue(1, 0).GetValue<duckdb::string>(),
	};

	if (type == duckdb::LogicalTypeId::DECIMAL) {
		to_column.width = result->GetValue(2, 0).GetValue<uint8_t>();
		to_column.scale = result->GetValue(3, 0).GetValue<uint8_t>();
	}

	TransactionContext transaction_context(con);

	add_column(con, table, to_column, "copy_column add");
	run_query(con, "copy_column update",
	          "UPDATE " + table.to_escaped_string() + " SET " + KeywordHelper::WriteQuoted(to_name, '"') + " = " +
	              quoted_from,
	          "Could not copy column values");

	transaction_context.Commit();
}

void MdSqlGenerator::copy_table_to_history_mode(duckdb::Connection& con, const table_def& from_table,
                                                const table_def& to_table, const std::string& soft_deleted_column) {
	const std::string from_table_name = from_table.to_escaped_string();
	const std::string to_table_name = to_table.to_escaped_string();

	// This already runs inside a transaction.
	// There is no need to add _fivetran_start as an additional primary key at
	// this point, as that happens in the migrate_*_to_history() below.
	copy_table(con, from_table, to_table, "copy_table_to_history_mode copy_table");

	if (!soft_deleted_column.empty()) {
		migrate_soft_delete_to_history(con, to_table, soft_deleted_column);
	} else {
		migrate_live_to_history(con, to_table);
	}
}

void MdSqlGenerator::rename_table(duckdb::Connection& con, const table_def& from_table,
                                  const std::string& to_table_name, const std::string& log_prefix) {
	std::ostringstream sql;
	sql << "ALTER TABLE " << from_table.to_escaped_string() << " RENAME TO "
	    << KeywordHelper::WriteQuoted(to_table_name, '"');

	run_query(con, log_prefix, sql.str(), "Could not rename table <" + from_table.to_escaped_string() + ">");
}

void MdSqlGenerator::rename_column(duckdb::Connection& con, const table_def& table, const std::string& from_column,
                                   const std::string& to_column) {
	const std::string absolute_table_name = table.to_escaped_string();
	std::ostringstream sql;
	sql << "ALTER TABLE " << absolute_table_name << " RENAME COLUMN " << KeywordHelper::WriteQuoted(from_column, '"')
	    << " TO " << KeywordHelper::WriteQuoted(to_column, '"');

	run_query(con, "rename_column", sql.str(),
	          "Could not rename column <" + from_column + "> to <" + to_column + "> in table <" + absolute_table_name +
	              ">");
}

bool MdSqlGenerator::history_table_is_valid(duckdb::Connection& con, const std::string& absolute_table_name,
                                            const std::string& quoted_timestamp) {
	// This performs the "Validation before starting the migration" part of
	// add/drop column in history mode as specified in the docs:
	// https://github.com/fivetran/fivetran_partner_sdk/blob/bdaea1a/schema-migration-helper-service.md
	//
	// The semantics are that we throw an exception when we encounter an error
	// performing the check and when the data is inconsistent with respect to the
	// _fivetran_start values, and we return false when the table is empty, and
	// else we return true. This also allows us to cleanly redirect to performing
	// a regular add/drop column when the table is empty.

	auto result = con.Query("SELECT COUNT(*) FROM " + absolute_table_name);

	if (result->HasError()) {
		throw std::runtime_error("Could not query table size: " + result->GetError());
	}

	if (result->GetValue(0, 0).GetValue<int64_t>() == 0) {
		// The table is empty
		return false;
	}

	auto max_result = con.Query("SELECT MAX(\"_fivetran_start\") <= " + quoted_timestamp + " FROM " +
	                            absolute_table_name + " WHERE \"_fivetran_active\" = true");

	if (max_result->HasError()) {
		throw std::runtime_error("Could not query _fivetran_start value: " + max_result->GetError());
	}

	if (max_result->GetValue(0, 0).GetValue<bool>() != true) {
		throw std::runtime_error("The _fivetran_start column contains values larger "
		                         "than the operation timestamp. Please contact Fivetran support.");
	}

	return true;
}

void MdSqlGenerator::add_column_in_history_mode(duckdb::Connection& con, const table_def& table,
                                                const column_def& column, const std::string& operation_timestamp,
                                                const std::string& default_value) {
	const std::string absolute_table_name = table.to_escaped_string();

	const std::string quoted_timestamp = KeywordHelper::WriteQuoted(operation_timestamp, '\'') + "::TIMESTAMPTZ";

	TransactionContext transaction_context(con);
	add_column(con, table, column, "add_column_in_history_mode create");

	if (!history_table_is_valid(con, absolute_table_name, quoted_timestamp)) {
		// The table is empty and the column has been added
		transaction_context.Commit();
		return;
	}

	std::string casted_default_value =
	    "CAST(" + KeywordHelper::WriteQuoted(default_value, '\'') + " AS " + format_type(column) + ")";

	const std::string quoted_column = KeywordHelper::WriteQuoted(column.name, '"');

	{
		// Insert new rows with the default value, capturing the DDL change
		std::ostringstream sql;
		sql << "INSERT INTO " << absolute_table_name << " SELECT * REPLACE (" << casted_default_value << " AS "
		    << quoted_column << ", " << quoted_timestamp << " AS \"_fivetran_start\")"
		    << " FROM " << absolute_table_name << " WHERE \"_fivetran_active\" = TRUE AND _fivetran_start < "
		    << quoted_timestamp;

		run_query(con, "add_column_in_history_mode insert", sql.str(),
		          "Could not insert new rows for add_column_in_history_mode");
	}

	{
		// Per the docs:
		//   This step is important in case of source connector sends multiple
		//   ADD_COLUMN_IN_HISTORY_MODE operations with the same
		//   operation_timestamp. It will ensure, we only record history once for
		//   that timestamp.
		// Also see drop_column_in_history_mode(): this ensures that if we already
		// inserted records for the current operation_timestamp, we set the right
		// default value for the column we're currently processing.
		std::ostringstream sql;
		sql << "UPDATE " << absolute_table_name << " SET " << quoted_column << " = " << casted_default_value
		    << " WHERE \"_fivetran_start\" = " << quoted_timestamp;

		run_query(con, "add_column_in_history_mode update_new", sql.str(),
		          "Could not update new rows for add_column_in_history_mode");
	}

	{
		// Update previous active records
		std::ostringstream sql;
		sql << "UPDATE " << absolute_table_name << " SET \"_fivetran_active\" = FALSE,"
		    << " \"_fivetran_end\" = (" << quoted_timestamp << "::TIMESTAMP - (INTERVAL '1 millisecond'))"
		    << " WHERE \"_fivetran_active\" = TRUE"
		    << " AND \"_fivetran_start\" < " << quoted_timestamp;

		run_query(con, "add_column_in_history_mode update", sql.str(),
		          "Could not update records for add_column_in_history_mode");
	}

	transaction_context.Commit();
}

void MdSqlGenerator::update_column_value(duckdb::Connection& con, const table_def& table, const std::string& column,
                                         const std::string& value) {
	const std::string absolute_table_name = table.to_escaped_string();
	const std::string quoted_column = KeywordHelper::WriteQuoted(column, '"');

	std::ostringstream sql;

	if (value == "NULL") {
		// As per a discussion with Fivetran, if value == "NULL" we should interpret
		// this as an actual NULL. Varchar columns hence cannot be updated with the
		// string 'NULL' here.
		sql << "UPDATE " << absolute_table_name << " SET " << quoted_column << " = NULL";
	} else {
		sql << "UPDATE " << absolute_table_name << " SET " << quoted_column << " = "
		    << KeywordHelper::WriteQuoted(value, '\'');
	}

	run_query(con, "update_column_value", sql.str(),
	          "Could not update column <" + column + "> in table <" + absolute_table_name + ">");
}

void MdSqlGenerator::migrate_soft_delete_to_live(duckdb::Connection& con, const table_def& table,
                                                 const std::string& soft_deleted_column) {
	const std::string absolute_table_name = table.to_escaped_string();
	const std::string quoted_deleted_col = KeywordHelper::WriteQuoted(soft_deleted_column, '"');

	// Note: we cannot wrap these queries in a transaction because of duckdb issue
	// #20570

	// Delete rows where soft_deleted_column = TRUE
	{
		std::ostringstream sql;
		sql << "DELETE FROM " << absolute_table_name << " WHERE " << quoted_deleted_col << " = TRUE";
		run_query(con, "migrate_soft_delete_to_live delete", sql.str(), "Could not delete soft-deleted rows");
	}

	// Always drop the _fivetran_deleted column, with IF EXISTS as a safeguard
	drop_column(con, table, "_fivetran_deleted", "migrate_soft_delete_to_live drop", true);
}

void MdSqlGenerator::migrate_soft_delete_to_history(duckdb::Connection& con, const table_def& original_table,
                                                    const std::string& soft_deleted_column) {
	const std::string absolute_table_name = original_table.to_escaped_string();
	const std::string quoted_deleted_col = KeywordHelper::WriteQuoted(soft_deleted_column, '"');

	table_def temp_table {original_table.db_name, original_table.schema_name, original_table.table_name + "_temp"};
	const std::string temp_absolute_table_name = temp_table.to_escaped_string();

	{
		TransactionContext transaction_context(con);

		add_column(con, original_table,
		           column_def {.name = "_fivetran_start", .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
		           "migrate_soft_delete_to_history add_start");
		add_column(con, original_table,
		           column_def {.name = "_fivetran_end", .type = duckdb::LogicalTypeId::TIMESTAMP_TZ},
		           "migrate_soft_delete_to_history add_end");
		add_column(con, original_table,
		           column_def {
		               .name = "_fivetran_active",
		               .type = duckdb::LogicalTypeId::BOOLEAN,
		               .column_default = "true",
		           },
		           "migrate_soft_delete_to_history add_active");

		{
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
			std::string sql = fmt::format(query_template, absolute_table_name, quoted_deleted_col);
			run_query(con, "migrate_soft_delete_to_history update", sql, "Could not set history column values");
		}

		transaction_context.Commit();
	}

	{
		// See duckdb issue #20570: we can only start the transaction here at this
		// point.
		TransactionContext transaction_context(con);

		// Always drop the _fivetran_deleted column, with IF EXISTS as a safeguard
		drop_column(con, original_table, "_fivetran_deleted", "migrate_soft_delete_to_history drop", true);

		// Rename, copy and drop the original table to replace the primary key
		rename_table(con, original_table, temp_table.table_name, "migrate_soft_delete_to_history rename");

		std::vector<const column_def*> additional_pks;
		column_def fivetran_start {.name = "_fivetran_start", .type = duckdb::LogicalTypeId::TIMESTAMP_TZ};
		additional_pks.push_back(&fivetran_start);

		copy_table(con, temp_table, original_table, "migrate_soft_delete_to_history copy", additional_pks);
		drop_table(con, temp_table, "migrate_soft_delete_to_history drop");

		transaction_context.Commit();
	}
}

void MdSqlGenerator::migrate_history_to_soft_delete(duckdb::Connection& con, const table_def& table,
                                                    const std::string& soft_deleted_column) {
	const std::string quoted_deleted_col = KeywordHelper::WriteQuoted(soft_deleted_column, '"');

	TransactionContext transaction_context(con);

	// From the duckdb docs:
	// "ADD CONSTRAINT and DROP CONSTRAINT clauses are not yet supported in
	// DuckDB." In particular, we cannot drop the primary key constraint from the
	// original table. Hence, we need to create a new table.
	table_def temp_table {table.db_name, table.schema_name, table.table_name + "_temp"};
	const std::string temp_table_name = temp_table.to_escaped_string();

	std::vector<const column_def*> columns_pk;
	std::vector<const column_def*> columns_regular;
	const auto columns = describe_table(con, table);
	find_primary_keys(columns, columns_pk, &columns_regular, "_fivetran_start");

	if (columns_pk.empty()) {
		throw std::runtime_error("History table has no primary keys except "
		                         "_fivetran_start. Please contact "
		                         "Fivetran support.");
	}

	if (soft_deleted_column == "_fivetran_deleted") {
		std::ostringstream sql;
		sql << "CREATE TABLE " << temp_table_name
		    << " AS SELECT * EXCLUDE (\"_fivetran_start\", \"_fivetran_end\", \"_fivetran_active\"), "
		       "NOT \"_fivetran_active\" AS \"_fivetran_deleted\" FROM "
		    << table.to_escaped_string();

		// Keep only the latest record for a primary key based on the highest _fivetran_start, using QUALIFY
		sql << " QUALIFY row_number() OVER (partition by ";
		write_joined(sql, columns_pk, print_column);
		sql << " ORDER BY \"_fivetran_start\" DESC) = 1";

		run_query(con, "migrate_history_to_soft_delete create", sql.str(), "Could not create soft_deleted table");
	} else {
		std::ostringstream sql;
		sql << "CREATE TABLE " << temp_table_name
		    << " AS SELECT * EXCLUDE (\"_fivetran_start\", \"_fivetran_end\", \"_fivetran_active\") "
		       " REPLACE (NOT \"_fivetran_active\" AS "
		    << quoted_deleted_col << "), false as \"_fivetran_deleted\" FROM " << table.to_escaped_string();

		sql << " QUALIFY row_number() OVER (partition by ";
		write_joined(sql, columns_pk, print_column);
		sql << " ORDER BY \"_fivetran_start\" DESC) = 1";

		run_query(con, "migrate_history_to_soft_delete create", sql.str(), "Could not create soft_deleted table");
		// The quoted_deleted_col does not need an explicit default to be set here, it will inherit a default from the
		// original table below when we apply add_defaults
	}

	add_defaults(con,
	             {column_def {
	                 .name = "_fivetran_deleted",
	                 .type = duckdb::LogicalTypeId::BOOLEAN,
	                 .column_default = "false",
	             }},
	             temp_table_name, "migrate_history_to_soft_delete set_deleted_default");

	// _fivetran_start, _fivetran_end and _fivetran_active are not present in temp_table.
	std::vector<column_def> new_columns;
	for (auto& col : columns) {
		if (col.name != "_fivetran_start" && col.name != "_fivetran_end" && col.name != "_fivetran_active") {
			new_columns.push_back(col);
		}
	}
	add_defaults(con, new_columns, temp_table_name, "migrate_history_to_soft_delete set_default");
	add_pks(con, columns_pk, temp_table_name, "migrate_history_to_soft_delete set_pk");

	// Swap the original and temporary table
	drop_table(con, table, "migrate_history_to_soft_delete drop");
	rename_table(con, temp_table, table.table_name, "migrate_history_to_soft_delete rename");

	transaction_context.Commit();
}

void MdSqlGenerator::migrate_history_to_live(duckdb::Connection& con, const table_def& table, bool keep_deleted_rows) {
	const std::string absolute_table_name = table.to_escaped_string();

	TransactionContext transaction_context(con);

	// Optionally delete inactive rows
	if (!keep_deleted_rows) {
		run_query(con, "migrate_history_to_live delete",
		          "DELETE FROM " + absolute_table_name + " WHERE \"_fivetran_active\" = FALSE",
		          "Could not delete inactive rows");
	}

	// From the duckdb docs:
	// "ADD CONSTRAINT and DROP CONSTRAINT clauses are not yet supported in
	// DuckDB." In particular, we cannot drop the primary key constraint from the
	// original table. Hence, we need to create a new table.
	table_def temp_table {table.db_name, table.schema_name, table.table_name + "_temp"};
	const std::string temp_table_name = temp_table.to_escaped_string();

	{
		// Combine steps 1 and 3
		std::ostringstream sql;
		sql << "CREATE TABLE " << temp_table_name
		    << " AS SELECT * EXCLUDE (\"_fivetran_start\", \"_fivetran_end\", "
		       "\"_fivetran_active\") "
		    << " FROM " << absolute_table_name << ";";
		run_query(con, "migrate_history_to_live create", sql.str(), "Could not add soft_deleted_column");
	}

	auto columns = describe_table(con, table);

	std::vector<const column_def*> columns_pk;
	std::vector<const column_def*> columns_regular;
	find_primary_keys(columns, columns_pk, &columns_regular, "_fivetran_start");

	// _fivetran_start, _fivetran_end and _fivetran_active are not present in temp_table.
	std::vector<column_def> new_columns;
	for (auto& col : columns) {
		if (col.name != "_fivetran_start" && col.name != "_fivetran_end" && col.name != "_fivetran_active") {
			new_columns.push_back(col);
		}
	}
	add_defaults(con, new_columns, temp_table_name, "migrate_history_to_live set_default");

	if (!keep_deleted_rows) {
		add_pks(con, columns_pk, temp_table_name, "migrate_history_to_live add_pks");
	}

	// Swap the original and temporary table
	drop_table(con, table, "migrate_history_to_live drop");
	rename_table(con, temp_table, table.table_name, "migrate_history_to_live rename");

	transaction_context.Commit();
}

void MdSqlGenerator::migrate_live_to_soft_delete(duckdb::Connection& con, const table_def& table,
                                                 const std::string& soft_deleted_column) {
	const std::string absolute_table_name = table.to_escaped_string();
	const std::string quoted_deleted_col = KeywordHelper::WriteQuoted(soft_deleted_column, '"');

	TransactionContext transaction_context(con);

	add_column(con, table,
	           column_def {
	               .name = soft_deleted_column,
	               .type = duckdb::LogicalTypeId::BOOLEAN,
	           },
	           "migrate_live_to_soft_delete add", true);

	// Set all existing rows to not deleted
	run_query(con, "migrate_live_to_soft_delete update",
	          "UPDATE " + absolute_table_name + " SET " + quoted_deleted_col + " = FALSE WHERE " + quoted_deleted_col +
	              " IS NULL",
	          "Could not set soft_deleted_column values");

	transaction_context.Commit();
}

void MdSqlGenerator::migrate_live_to_history(duckdb::Connection& con, const table_def& table) {
	const std::string absolute_table_name = table.to_escaped_string();
	table_def temp_table {table.db_name, table.schema_name, table.table_name + "_temp"};
	const std::string temp_absolute_table_name = temp_table.to_escaped_string();

	TransactionContext transaction_context(con);

	add_column(con, table,
	           column_def {
	               .name = "_fivetran_start",
	               .type = duckdb::LogicalTypeId::TIMESTAMP_TZ,
	           },
	           "migrate_live_to_history add_start");
	add_column(con, table,
	           column_def {
	               .name = "_fivetran_end",
	               .type = duckdb::LogicalTypeId::TIMESTAMP_TZ,
	           },
	           "migrate_live_to_history add_end");
	add_column(con, table,
	           column_def {
	               .name = "_fivetran_active",
	               .type = duckdb::LogicalTypeId::BOOLEAN,
	               .column_default = "true",
	           },
	           "migrate_live_to_history add_active");

	// Set all records as active
	run_query(con, "migrate_live_to_history update",
	          "UPDATE " + absolute_table_name +
	              " SET \"_fivetran_start\" = NOW(),"
	              " \"_fivetran_end\" = '9999-12-31T23:59:59.999Z'::TIMESTAMPTZ,"
	              " \"_fivetran_active\" = TRUE",
	          "Could not set history column values");

	// Rename, copy and drop the original table to be able to replace the primary
	// key
	rename_table(con, table, temp_table.table_name, "migrate_live_to_history rename");

	std::vector<const column_def*> additional_pks;
	column_def fivetran_start {.name = "_fivetran_start", .type = duckdb::LogicalTypeId::TIMESTAMP_TZ};
	additional_pks.push_back(&fivetran_start);

	copy_table(con, temp_table, table, "migrate_live_to_history copy", additional_pks);
	drop_table(con, temp_table, "migrate_live_to_history drop");

	transaction_context.Commit();
}
