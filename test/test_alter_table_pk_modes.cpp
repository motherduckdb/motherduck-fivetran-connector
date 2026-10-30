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

bool column_is_not_null(duckdb::Connection& con, const std::string& column) {
	return scalar(con, "SELECT NOT is_nullable FROM duckdb_columns() WHERE table_name = 't' AND column_name = '" +
	                       column + "'") == "true";
}

const table_def TABLE {"memory", "main", "t"};
const std::string QUALIFIED = "\"memory\".\"main\".\"t\"";

column_def key_col(const std::string& name, duckdb::LogicalTypeId type) {
	return column_def {.name = name, .type = type, .primary_key = true};
}
column_def col(const std::string& name, duckdb::LogicalTypeId type) {
	return column_def {.name = name, .type = type};
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

TEST_CASE("AlterTable in NotNull mode recreates a strict-mode table when widening the key", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::NotNull;

	// Table created in strict mode: enforced PRIMARY KEY on id.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER, \"v\" VARCHAR, PRIMARY KEY (\"id\"))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO " + QUALIFIED + " VALUES (1, 'a')"));

	// Widen the key to (id, region) by adding a new key column.
	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER),
	                                     col("v", duckdb::LogicalTypeId::VARCHAR),
	                                     key_col("region", duckdb::LogicalTypeId::VARCHAR)};
	generator.alter_table(con, TABLE, requested, /*drop_columns=*/false, pk_mode);

	// The strict-mode PK is gone (recreated without one) and the new key column is NOT NULL.
	REQUIRE(primary_key_count(con) == 0);
	REQUIRE(column_is_not_null(con, "region"));
	REQUIRE(column_is_not_null(con, "id"));
	REQUIRE(row_count(con, QUALIFIED) == 1); // existing row preserved

	// Data non-unique on the OLD pk (id) but unique on the new key now inserts fine.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO " + QUALIFIED + " VALUES (2, 'x', 'us'), (2, 'y', 'eu')"));
	REQUIRE(row_count(con, QUALIFIED) == 3);
}

TEST_CASE("AlterTable in NotNull mode recreates a strict-mode table when dropping a key column", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::NotNull;

	// Table created in strict mode: composite PRIMARY KEY (id, b).
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER, \"b\" INTEGER, PRIMARY KEY (\"id\", \"b\"))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO " + QUALIFIED + " VALUES (1, 1)"));

	// Drop b (part of the old PK); the key becomes just id.
	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER)};
	generator.alter_table(con, TABLE, requested, /*drop_columns=*/true, pk_mode);

	// No Catalog Error; b is gone, the PK is gone, and the row is preserved.
	REQUIRE(primary_key_count(con) == 0);
	REQUIRE(scalar(con, "SELECT count(*) FROM duckdb_columns() WHERE table_name = 't' AND column_name = 'b'") == "0");
	REQUIRE(column_is_not_null(con, "id"));
	REQUIRE(row_count(con, QUALIFIED) == 1);
}

TEST_CASE("AlterTable changes the key in place for a NOT NULL table (no PRIMARY KEY, no recreate needed)", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::NotNull;

	// Table created in NotNull mode: NOT NULL key, no PRIMARY KEY.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER NOT NULL, \"v\" VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO " + QUALIFIED + " VALUES (1, 'a')"));

	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER),
	                                     col("v", duckdb::LogicalTypeId::VARCHAR),
	                                     key_col("region", duckdb::LogicalTypeId::VARCHAR)};
	generator.alter_table(con, TABLE, requested, /*drop_columns=*/false, pk_mode);

	// Still no PRIMARY KEY (we never introduce one in NotNull mode); the new key
	// column is NOT NULL; the existing row is preserved in place.
	REQUIRE(primary_key_count(con) == 0);
	REQUIRE(column_is_not_null(con, "region"));
	REQUIRE(row_count(con, QUALIFIED) == 1);
	// Non-unique on id but unique on (id, region) inserts fine.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO " + QUALIFIED + " VALUES (2, 'x', 'us'), (2, 'y', 'eu')"));
	REQUIRE(row_count(con, QUALIFIED) == 3);
}

TEST_CASE("AlterTable in strict mode rebuilds the PRIMARY KEY over the new key", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::Strict;

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER, \"v\" VARCHAR, PRIMARY KEY (\"id\"))"));
	// unique ids -> new key stays unique
	REQUIRE_NO_FAIL(con.Query("INSERT INTO " + QUALIFIED + " VALUES (1, 'a'), (2, 'b')"));

	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER),
	                                     col("v", duckdb::LogicalTypeId::VARCHAR),
	                                     key_col("region", duckdb::LogicalTypeId::VARCHAR)};
	generator.alter_table(con, TABLE, requested, /*drop_columns=*/false, pk_mode);

	// Strict mode re-adds an enforced PRIMARY KEY (now over the new key set), so a
	// duplicate on (id, region) is rejected.
	REQUIRE(primary_key_count(con) == 1);
	REQUIRE(row_count(con, QUALIFIED) == 2);
	auto dup = con.Query("INSERT INTO " + QUALIFIED + " VALUES (1, 'c', '')"); // (1, '') already exists
	REQUIRE(dup->HasError());
}

TEST_CASE("AlterTable in strict mode adds the PRIMARY KEY in place to a NotNull-mode table", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER NOT NULL, \"v\" VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO " + QUALIFIED + " VALUES (1, 'a'), (2, 'b')"));
	const std::string table_oid_query = "SELECT table_oid FROM duckdb_tables() WHERE table_name = 't'";
	const auto table_oid = scalar(con, table_oid_query);

	// The key stays the same, the request only adds a regular column.
	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER),
	                                     col("v", duckdb::LogicalTypeId::VARCHAR),
	                                     col("w", duckdb::LogicalTypeId::VARCHAR)};
	generator.alter_table(con, TABLE, requested, /*drop_columns=*/false, PrimaryKeyMode::Strict);

	REQUIRE(primary_key_count(con) == 1);
	REQUIRE(row_count(con, QUALIFIED) == 2);
	REQUIRE_FALSE(column_is_not_null(con, "w"));
	// A recreated table would have a new oid.
	REQUIRE(scalar(con, table_oid_query) == table_oid);
}

TEST_CASE("AlterTable in strict mode changes the key of a NotNull-mode table in place", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER NOT NULL, \"v\" VARCHAR NOT NULL)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO " + QUALIFIED + " VALUES (1, 'a'), (2, 'b')"));
	const std::string table_oid_query = "SELECT table_oid FROM duckdb_tables() WHERE table_name = 't'";
	const auto table_oid = scalar(con, table_oid_query);

	// v leaves the key and region joins it.
	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER),
	                                     col("v", duckdb::LogicalTypeId::VARCHAR),
	                                     key_col("region", duckdb::LogicalTypeId::VARCHAR)};
	generator.alter_table(con, TABLE, requested, /*drop_columns=*/false, PrimaryKeyMode::Strict);

	REQUIRE(primary_key_names(generator.describe_table(con, TABLE)) == std::vector<std::string> {"id", "region"});
	REQUIRE(primary_key_count(con) == 1);
	REQUIRE(row_count(con, QUALIFIED) == 2);
	REQUIRE(scalar(con, table_oid_query) == table_oid);
	// The PRIMARY KEY covers the new key, so a duplicate on (id, region) is rejected.
	REQUIRE(con.Query("INSERT INTO " + QUALIFIED + " VALUES (1, NULL, '')")->HasError());
}

TEST_CASE("AlterTable in strict mode raises a task when a NotNull-mode table contains duplicate keys", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER NOT NULL, \"v\" VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO " + QUALIFIED + " VALUES (1, 'a'), (1, 'b')"));

	// The new column w either leaves the key unchanged or joins it. Both rows get the same default for w, so the
	// key stays duplicated either way.
	const bool w_is_key = GENERATE(false, true);
	CAPTURE(w_is_key);
	std::vector<column_def> requested = {
	    key_col("id", duckdb::LogicalTypeId::INTEGER), col("v", duckdb::LogicalTypeId::VARCHAR),
	    w_is_key ? key_col("w", duckdb::LogicalTypeId::VARCHAR) : col("w", duckdb::LogicalTypeId::VARCHAR)};

	std::string task_message;
	try {
		generator.alter_table(con, TABLE, requested, /*drop_columns=*/false, PrimaryKeyMode::Strict);
		FAIL("expected a RecoverableError");
	} catch (const md_error::RecoverableError& e) {
		task_message = e.what();
	}
	REQUIRE_THAT(task_message, Catch::Matchers::ContainsSubstring("contains duplicate primary keys"));

	// Rolled back: no PRIMARY KEY, no new column, both rows survive.
	REQUIRE(primary_key_count(con) == 0);
	REQUIRE(scalar(con, "SELECT count(*) FROM duckdb_columns() WHERE table_name = 't' AND column_name = 'w'") == "0");
	REQUIRE(row_count(con, QUALIFIED) == 2);
}
