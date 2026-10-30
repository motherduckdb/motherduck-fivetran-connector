// Unit tests that check the DuckDB MERGE INTO semantics the connector's
// upsert path relies on. These run entirely against an in-memory DuckDB (no
// MotherDuck) to catch changed behavior when updating DuckDB.

#include "duckdb.hpp"
#include "integration/common.hpp"
#include "md_logging.hpp"
#include "schema_types.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>
#include <vector>

namespace {

int64_t row_count(duckdb::Connection& con, const std::string& table) {
	const auto res = con.Query("SELECT count(*) FROM " + table);
	REQUIRE_FALSE(res->HasError());
	return res->GetValue(0, 0).GetValue<int64_t>();
}

std::string scalar(duckdb::Connection& con, const std::string& query) {
	const auto res = con.Query(query);
	REQUIRE_FALSE(res->HasError());
	return res->GetValue(0, 0).ToString();
}
} // namespace

TEST_CASE("MERGE upserts: matched rows update, unmatched rows insert", "[merge]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE target (id INTEGER NOT NULL, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO target VALUES (1, 'old')"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE source (id INTEGER, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO source VALUES (1, 'new'), (2, 'inserted')"));

	REQUIRE_NO_FAIL(con.Query("MERGE INTO target USING source ON target.id = source.id "
	                          "WHEN MATCHED THEN UPDATE SET v = source.v "
	                          "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v)"));

	REQUIRE(row_count(con, "target") == 2);
	REQUIRE(scalar(con, "SELECT v FROM target WHERE id = 1") == "new");      // matched -> updated
	REQUIRE(scalar(con, "SELECT v FROM target WHERE id = 2") == "inserted"); // unmatched -> inserted
}

TEST_CASE("MERGE upsert is idempotent when the same batch is retried", "[merge]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE target (id INTEGER NOT NULL, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE source (id INTEGER, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO source VALUES (1, 'a'), (2, 'b')"));

	for (unsigned int i = 0; i < 3; i++) {
		CAPTURE(i);
		REQUIRE_NO_FAIL(con.Query("MERGE INTO target USING source ON target.id = source.id "
		                          "WHEN MATCHED THEN UPDATE SET v = source.v "
		                          "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v)"));
		REQUIRE(row_count(con, "target") == 2);
		REQUIRE(scalar(con, "SELECT v FROM target WHERE id = 1") == "a");
	}
}

TEST_CASE("Without a constraint, a duplicate key within one MERGE batch is inserted twice", "[merge]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	// NOT NULL only, no key constraint
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE target (id INTEGER NOT NULL, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE source (id INTEGER, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO source VALUES (1, 'x'), (1, 'y')")); // duplicate key, both new

	REQUIRE_NO_FAIL(con.Query("MERGE INTO target USING source ON target.id = source.id "
	                          "WHEN MATCHED THEN UPDATE SET v = source.v "
	                          "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v)"));

	// Both source rows land under the same key: the target had no row for id=1,
	// so neither source row matched, and each took the INSERT branch.
	// This is the only way MERGE can introduce a duplicate.
	// Fivetran de-duplicates  within a batch, so this should not occur in practice.
	REQUIRE(row_count(con, "target") == 2);
	REQUIRE(scalar(con, "SELECT count(*) FROM target WHERE id = 1") == "2");
	REQUIRE(scalar(con, "SELECT string_agg(v, ',' ORDER BY v) FROM target WHERE id = 1") == "x,y");
}

TEST_CASE("A PRIMARY KEY constraint rejects a duplicate key within one MERGE batch", "[merge]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	// Strict primary key mode enforces uniqueness
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE target (id INTEGER PRIMARY KEY, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE source (id INTEGER, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO source VALUES (1, 'x'), (1, 'y')"));

	auto res = con.Query("MERGE INTO target USING source ON target.id = source.id "
	                     "WHEN MATCHED THEN UPDATE SET v = source.v "
	                     "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v)");
	REQUIRE(res->HasError()); // PRIMARY KEY / UNIQUE constraint violation
	REQUIRE(row_count(con, "target") == 0);
}

TEST_CASE("MERGE does not error when several source rows match one target row", "[merge]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE target (id INTEGER NOT NULL, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO target VALUES (1, 'orig')"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE source (id INTEGER, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO source VALUES (1, 'a'), (1, 'b')"));

	REQUIRE_NO_FAIL(con.Query("MERGE INTO target USING source ON target.id = source.id "
	                          "WHEN MATCHED THEN UPDATE SET v = source.v "
	                          "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v)"));

	// One value from `source` wins
	REQUIRE(row_count(con, "target") == 1);
}

TEST_CASE("MERGE matches on a composite key across all key columns", "[merge]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE target (a INTEGER NOT NULL, b INTEGER NOT NULL, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO target VALUES (1, 1, 'old')"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE source (a INTEGER, b INTEGER, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO source VALUES (1, 1, 'match'), (1, 2, 'different-b')"));

	REQUIRE_NO_FAIL(con.Query("MERGE INTO target USING source ON target.a = source.a AND target.b = source.b "
	                          "WHEN MATCHED THEN UPDATE SET v = source.v "
	                          "WHEN NOT MATCHED THEN INSERT (a, b, v) VALUES (source.a, source.b, source.v)"));

	REQUIRE(row_count(con, "target") == 2);
	REQUIRE(scalar(con, "SELECT v FROM target WHERE a = 1 AND b = 1") == "match");       // (1,1) matched -> updated
	REQUIRE(scalar(con, "SELECT v FROM target WHERE a = 1 AND b = 2") == "different-b"); // (1,2) unmatched -> inserted
}