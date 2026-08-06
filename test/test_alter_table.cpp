#include "duckdb.hpp"
#include "integration/common.hpp"
#include "md_logging.hpp"
#include "schema_types.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <vector>

TEST_CASE("AlterTable recreate preserves data in columns the request omits if drop_columns=false", "[alter]") {
	// "Deleted" columns are retained with drop_columns=false and their data must be carried over to the recreated
	// table.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR, to_be_deleted INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'a', 42)"));

	// "extra" is absent from the request. The widened primary key triggers a recreate.
	constexpr std::vector<column_def> requested = {
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
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR, to_be_deleted VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'a', 'remove-me')"));

	// "obsolete" is absent from the request and drop_columns allows dropping it,
	// so the recreated table must not carry it over.
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
