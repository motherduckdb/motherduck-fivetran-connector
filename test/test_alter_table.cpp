#include "duckdb.hpp"
#include "integration/common.hpp"
#include "md_logging.hpp"
#include "schema_types.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>
#include <vector>

namespace {
// The names of the key columns among <columns>, in table order.
std::vector<std::string> primary_key_names(const std::vector<column_def>& columns) {
	std::vector<const column_def*> columns_pk;
	find_primary_keys(columns, columns_pk);

	std::vector<std::string> names;
	names.reserve(columns_pk.size());
	for (const auto* col : columns_pk) {
		names.push_back(col->name);
	}
	return names;
}
} // namespace

TEST_CASE("AlterTable recreate preserves data in columns the request omits if drop_columns=false", "[alter]") {
	// "Deleted" columns are retained with drop_columns=false and their data must be carried over to the recreated
	// table.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR, to_be_deleted INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'a', 42)"));

	// "to_be_deleted" is absent from the request. The widened primary key triggers a recreate.
	const std::vector<column_def> requested = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	    column_def {.name = "v", .type = duckdb::LogicalTypeId::VARCHAR},
	    column_def {.name = "new_col", .type = duckdb::LogicalTypeId::VARCHAR, .primary_key = true}};
	generator.alter_table(con, table, requested, /*drop_columns=*/false);

	// "to_be_deleted" column is still there
	const auto columns = generator.describe_table(con, table);
	REQUIRE(columns.size() == 4);
	REQUIRE(columns[2].name == "to_be_deleted");

	auto res = con.Query("SELECT id, v, to_be_deleted FROM t");
	REQUIRE_NO_FAIL(res);
	REQUIRE(res->RowCount() == 1);
	check_row(res, 0, {duckdb::Value::INTEGER(1), "a", duckdb::Value::INTEGER(42)});
}

TEST_CASE("AlterTable recreate drops the columns the request drops if drop_columns=true", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR, to_be_deleted VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'a', 'remove-me')"));

	// "to_be_deleted" is absent from the request and drop_columns allows dropping
	// it, so the recreated table must not carry it over.
	const std::vector<column_def> requested = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	    column_def {.name = "v", .type = duckdb::LogicalTypeId::VARCHAR},
	    column_def {.name = "new_col", .type = duckdb::LogicalTypeId::VARCHAR, .primary_key = true}};
	generator.alter_table(con, table, requested, /*drop_columns=*/true);

	const auto columns = generator.describe_table(con, table);
	REQUIRE(columns.size() == 3);
	for (const auto& column : columns) {
		REQUIRE(column.name != "to_be_deleted");
	}

	auto res = con.Query("SELECT id, v FROM t");
	REQUIRE_NO_FAIL(res);
	REQUIRE(res->RowCount() == 1);
	check_row(res, 0, {duckdb::Value::INTEGER(1), "a"});
}

TEST_CASE("AlterTable removes the constraints of columns the request omits if drop_columns=false", "[alter]") {
	// A column the request no longer contains must not stay part of the primary
	// key, or WriteBatch's ON CONFLICT would no longer match it.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER, region VARCHAR, v VARCHAR, PRIMARY KEY (id, region))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'us', 'a'), (2, 'eu', 'b')"));

	// "region" is absent from the request. Narrowing the primary key is the only
	// reason to recreate here.
	const std::vector<column_def> requested = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	    column_def {.name = "v", .type = duckdb::LogicalTypeId::VARCHAR}};
	generator.alter_table(con, table, requested, /*drop_columns=*/false);

	REQUIRE(primary_key_names(generator.describe_table(con, table)) == std::vector<std::string> {"id"});

	// The column and its data are still there ...
	auto res = con.Query("SELECT id, region, v FROM t ORDER BY id");
	REQUIRE_NO_FAIL(res);
	REQUIRE(res->RowCount() == 2);
	check_row(res, 0, {duckdb::Value::INTEGER(1), "us", "a"});
	check_row(res, 1, {duckdb::Value::INTEGER(2), "eu", "b"});

	// ... but it no longer rejects NULL.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t (id, v) VALUES (3, 'c')"));
}
