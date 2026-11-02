#include "duckdb.hpp"
#include "integration/common.hpp"
#include "md_error.hpp"
#include "md_logging.hpp"
#include "schema_types.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <string>
#include <vector>

namespace {

int64_t row_count(duckdb::Connection& con, const std::string& table) {
	auto res = con.Query("SELECT count(*) FROM " + table);
	REQUIRE_FALSE(res->HasError());
	return res->GetValue(0, 0).GetValue<int64_t>();
}

std::string scalar(duckdb::Connection& con, const std::string& query) {
	auto res = con.Query(query);
	REQUIRE_FALSE(res->HasError());
	return res->GetValue(0, 0).ToString();
}

int64_t primary_key_count(duckdb::Connection& con) {
	const std::string query =
	    "SELECT count(*) FROM duckdb_constraints() WHERE table_name = 't' AND constraint_type = 'PRIMARY KEY'";
	return std::stoll(scalar(con, query));
}

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

TEST_CASE("ensure_primary_key_mode recreates a table without its PRIMARY KEY in NotNull mode", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	// Table created in strict mode: composite PRIMARY KEY (id, region).
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER, region VARCHAR, v VARCHAR, PRIMARY KEY (id, region))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'us', 'a')"));
	const std::string table_oid_query = "SELECT table_oid FROM duckdb_tables() WHERE table_name = 't'";
	const auto table_oid = scalar(con, table_oid_query);

	generator.ensure_primary_key_mode(con, table, PrimaryKeyMode::NotNull);

	// Recreated without the PRIMARY KEY. The key columns stay NOT NULL and the row is preserved.
	REQUIRE(primary_key_count(con) == 0);
	REQUIRE(scalar(con, table_oid_query) != table_oid);
	REQUIRE(primary_key_names(generator.describe_table(con, table)) == std::vector<std::string> {"id", "region"});
	auto res = con.Query("SELECT id, region, v FROM t");
	REQUIRE_NO_FAIL(res);
	REQUIRE(res->RowCount() == 1);
	check_row(res, 0, {duckdb::Value::INTEGER(1), "us", "a"});

	// A duplicate key is no longer rejected.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'us', 'b')"));
}

TEST_CASE("ensure_primary_key_mode adds the PRIMARY KEY in place in strict mode", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	// Table created in NotNull mode
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'a'), (2, 'b')"));
	const std::string table_oid_query = "SELECT table_oid FROM duckdb_tables() WHERE table_name = 't'";
	const auto table_oid = scalar(con, table_oid_query);

	generator.ensure_primary_key_mode(con, table, PrimaryKeyMode::Strict);

	REQUIRE(primary_key_count(con) == 1);
	REQUIRE(row_count(con, "t") == 2);
	// A recreated table would have a new oid.
	REQUIRE(scalar(con, table_oid_query) == table_oid);
	auto dup = con.Query("INSERT INTO t VALUES (1, 'c')");
	REQUIRE_THAT(dup->GetError(),
	             Catch::Matchers::ContainsSubstring("Duplicate key \"id: 1\" violates primary key constraint"));
}

TEST_CASE("ensure_primary_key_mode raises a task in strict mode when the table contains duplicate keys", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'a'), (1, 'b')"));

	std::string task_message;
	try {
		generator.ensure_primary_key_mode(con, table, PrimaryKeyMode::Strict);
		FAIL("expected a RecoverableError");
	} catch (const md_error::RecoverableError& e) {
		task_message = e.what();
	}
	REQUIRE_THAT(task_message, Catch::Matchers::ContainsSubstring("contains duplicate primary keys"));

	// Rolled back: no PRIMARY KEY, both rows survive.
	REQUIRE(primary_key_count(con) == 0);
	REQUIRE(row_count(con, "t") == 2);
}

TEST_CASE("ensure_primary_key_mode leaves tables that match the mode untouched", "[alter]") {
	const bool strict = GENERATE(false, true);
	CAPTURE(strict);
	const PrimaryKeyMode pk_mode = strict ? PrimaryKeyMode::Strict : PrimaryKeyMode::NotNull;

	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query(strict ? "CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR)"
	                                 : "CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR)"));
	// Without key columns, a table matches both modes.
	const table_def keyless_table {"memory", "main", "keyless"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE keyless (v VARCHAR)"));
	const std::string table_oids_query =
	    "SELECT string_agg(table_oid::VARCHAR, ',' ORDER BY table_name) FROM duckdb_tables()";
	const auto table_oids = scalar(con, table_oids_query);

	generator.ensure_primary_key_mode(con, table, pk_mode);
	generator.ensure_primary_key_mode(con, keyless_table, pk_mode);
	// A missing table is left to the request itself.
	generator.ensure_primary_key_mode(con, table_def {"memory", "main", "missing"}, pk_mode);

	REQUIRE(scalar(con, table_oids_query) == table_oids);
	REQUIRE(primary_key_count(con) == (strict ? 1 : 0));
	REQUIRE(scalar(con, "SELECT count(*) FROM duckdb_constraints() WHERE table_name = 'keyless' AND "
	                    "constraint_type = 'PRIMARY KEY'") == "0");
}
