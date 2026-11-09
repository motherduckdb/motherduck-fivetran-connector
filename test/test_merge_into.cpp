// Unit tests that check the DuckDB MERGE INTO semantics the connector's
// upsert path relies on. These run entirely against an in-memory DuckDB (no
// MotherDuck) to catch changed behavior when updating DuckDB.

#include "duckdb.hpp"
#include "integration/common.hpp"
#include "md_logging.hpp"
#include "schema_types.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
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

TEST_CASE("MERGE inserts a duplicate key within one batch twice unless a PRIMARY KEY rejects it", "[merge]") {
	// Two source rows with the same key that the target does not have yet both take the INSERT branch. This is the
	// only way MERGE can introduce a duplicate. Fivetran de-duplicates within a batch, so this should not occur in
	// practice.
	const bool has_primary_key = GENERATE(false, true);
	CAPTURE(has_primary_key);

	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	REQUIRE_NO_FAIL(con.Query(has_primary_key ? "CREATE TABLE target (id INTEGER PRIMARY KEY, v VARCHAR)"
	                                          : "CREATE TABLE target (id INTEGER NOT NULL, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE source (id INTEGER, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO source VALUES (1, 'x'), (1, 'y')"));

	const auto res = con.Query("MERGE INTO target USING source ON target.id = source.id "
	                           "WHEN MATCHED THEN UPDATE SET v = source.v "
	                           "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v)");
	if (has_primary_key) {
		// Rejected by the constraint and rolled back.
		REQUIRE(res->HasError());
		REQUIRE(row_count(con, "target") == 0);
	} else {
		REQUIRE_NO_FAIL(res);
		REQUIRE(row_count(con, "target") == 2);
		REQUIRE(scalar(con, "SELECT string_agg(v, ',' ORDER BY v) FROM target WHERE id = 1") == "x,y");
	}
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