// Unit tests that pin down the DuckDB MERGE INTO semantics the connector's
// upsert path relies on. These run entirely against an in-memory DuckDB (no
// MotherDuck), so they double as a regression guard: if a future DuckDB bump
// changes MERGE behavior, these fail loudly rather than silently corrupting
// upserts. The SQL shapes here mirror MdSqlGenerator::upsert.

#include "duckdb.hpp"
#include "md_logging.hpp"
#include "schema_types.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
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

void ok(duckdb::Connection& con, const std::string& query) {
	auto res = con.Query(query);
	INFO(query);
	REQUIRE_FALSE(res->HasError());
}

// Mirrors the MERGE that MdSqlGenerator::upsert emits: match on the key columns,
// update the regular columns on a match, insert the full row otherwise.
const char* UPSERT_MERGE = R"(
MERGE INTO t AS target USING staging AS source ON target."id" = source."id"
  WHEN MATCHED THEN UPDATE SET "v" = source."v"
  WHEN NOT MATCHED THEN INSERT ("id", "v") VALUES (source."id", source."v")
)";

} // namespace

TEST_CASE("MERGE upserts: matched rows update, unmatched rows insert", "[merge]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	ok(con, "CREATE TABLE t (\"id\" INTEGER NOT NULL, \"v\" VARCHAR)");
	ok(con, "INSERT INTO t VALUES (1, 'old')");
	ok(con, "CREATE TABLE staging (\"id\" INTEGER, \"v\" VARCHAR)");
	ok(con, "INSERT INTO staging VALUES (1, 'new'), (2, 'inserted')");

	ok(con, UPSERT_MERGE);

	REQUIRE(row_count(con, "t") == 2);
	REQUIRE(scalar(con, "SELECT v FROM t WHERE id = 1") == "new");      // matched -> updated
	REQUIRE(scalar(con, "SELECT v FROM t WHERE id = 2") == "inserted"); // unmatched -> inserted
}

TEST_CASE("MERGE upsert is idempotent when the same batch is retried", "[merge]") {
	// A retried WriteBatch replays the same staging rows. Because MERGE matches
	// on the key, the second run updates the already-present rows instead of
	// duplicating them.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	ok(con, "CREATE TABLE t (\"id\" INTEGER NOT NULL, \"v\" VARCHAR)");
	ok(con, "CREATE TABLE staging (\"id\" INTEGER, \"v\" VARCHAR)");
	ok(con, "INSERT INTO staging VALUES (1, 'a'), (2, 'b')");

	ok(con, UPSERT_MERGE);
	REQUIRE(row_count(con, "t") == 2);

	ok(con, UPSERT_MERGE); // retry the exact same batch
	REQUIRE(row_count(con, "t") == 2);
	REQUIRE(scalar(con, "SELECT v FROM t WHERE id = 1") == "a");
}

TEST_CASE("MERGE folds a key re-sent in a later batch into the existing row", "[merge]") {
	// Fivetran may re-send an already-synced record in a later sync. Since each
	// file is a separate MERGE and the second one sees the first's committed
	// rows, the re-sent key matches and updates rather than duplicating.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	ok(con, "CREATE TABLE t (\"id\" INTEGER NOT NULL, \"v\" VARCHAR)");
	ok(con, "CREATE TABLE staging (\"id\" INTEGER, \"v\" VARCHAR)");

	// batch 1
	ok(con, "INSERT INTO staging VALUES (1, 'a')");
	ok(con, UPSERT_MERGE);
	// batch 2 re-sends key 1 (updated) and adds key 2
	ok(con, "DELETE FROM staging");
	ok(con, "INSERT INTO staging VALUES (1, 'a-updated'), (2, 'b')");
	ok(con, UPSERT_MERGE);

	REQUIRE(row_count(con, "t") == 2);
	REQUIRE(scalar(con, "SELECT v FROM t WHERE id = 1") == "a-updated");
}

TEST_CASE("Without a constraint, a duplicate key within one MERGE batch is inserted twice", "[merge]") {
	// This is the ONLY way MERGE can introduce a duplicate: two rows with the same
	// key, both absent from the target, in a single batch. Fivetran de-duplicates
	// within a sync, so this should not occur in practice; strict mode (next test)
	// rejects it outright.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	ok(con, "CREATE TABLE t (\"id\" INTEGER NOT NULL, \"v\" VARCHAR)"); // NOT NULL only, no key constraint
	ok(con, "CREATE TABLE staging (\"id\" INTEGER, \"v\" VARCHAR)");
	ok(con, "INSERT INTO staging VALUES (1, 'x'), (1, 'y')"); // duplicate key, both new

	ok(con, UPSERT_MERGE);

	// Both source rows land under the same key: the target had no row for id=1,
	// so neither source row matched, and each took the INSERT branch.
	REQUIRE(row_count(con, "t") == 2);
	REQUIRE(scalar(con, "SELECT count(*) FROM t WHERE id = 1") == "2");
	REQUIRE(scalar(con, "SELECT string_agg(v, ',' ORDER BY v) FROM t WHERE id = 1") == "x,y");
}

TEST_CASE("A PRIMARY KEY constraint rejects a duplicate key within one MERGE batch", "[merge]") {
	// Strict mode keeps the enforced PRIMARY KEY, so the duplicate-in-one-batch
	// case above is rejected and rolled back rather than duplicated.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	ok(con, "CREATE TABLE t (\"id\" INTEGER PRIMARY KEY, \"v\" VARCHAR)");
	ok(con, "CREATE TABLE staging (\"id\" INTEGER, \"v\" VARCHAR)");
	ok(con, "INSERT INTO staging VALUES (1, 'x'), (1, 'y')");

	auto res = con.Query(UPSERT_MERGE);
	REQUIRE(res->HasError()); // PRIMARY KEY / UNIQUE constraint violation
	REQUIRE(row_count(con, "t") == 0);
}

TEST_CASE("MERGE does not error when several source rows match one target row", "[merge]") {
	// DuckDB does not raise the SQL-standard "cardinality violation" here; it
	// applies the updates and one value wins. The result is still a single row.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	ok(con, "CREATE TABLE t (\"id\" INTEGER NOT NULL, \"v\" VARCHAR)");
	ok(con, "INSERT INTO t VALUES (1, 'orig')");
	ok(con, "CREATE TABLE staging (\"id\" INTEGER, \"v\" VARCHAR)");
	ok(con, "INSERT INTO staging VALUES (1, 'a'), (1, 'b')");

	ok(con, UPSERT_MERGE);

	REQUIRE(row_count(con, "t") == 1);
}

TEST_CASE("MERGE matches on a composite key across all key columns", "[merge]") {
	// The connector joins on every primary key column (ON k1=k1 AND k2=k2).
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	ok(con, "CREATE TABLE t (\"a\" INTEGER NOT NULL, \"b\" INTEGER NOT NULL, \"v\" VARCHAR)");
	ok(con, "INSERT INTO t VALUES (1, 1, 'old')");
	ok(con, "CREATE TABLE staging (\"a\" INTEGER, \"b\" INTEGER, \"v\" VARCHAR)");
	ok(con, "INSERT INTO staging VALUES (1, 1, 'match'), (1, 2, 'different-b')");

	ok(con, R"(
	MERGE INTO t AS target USING staging AS source ON target."a" = source."a" AND target."b" = source."b"
	  WHEN MATCHED THEN UPDATE SET "v" = source."v"
	  WHEN NOT MATCHED THEN INSERT ("a", "b", "v") VALUES (source."a", source."b", source."v"))");

	REQUIRE(row_count(con, "t") == 2);
	REQUIRE(scalar(con, "SELECT v FROM t WHERE a = 1 AND b = 1") == "match");       // (1,1) matched -> updated
	REQUIRE(scalar(con, "SELECT v FROM t WHERE a = 1 AND b = 2") == "different-b"); // (1,2) unmatched -> inserted
}

TEST_CASE("MdSqlGenerator::upsert produces a retry-safe MERGE (no PRIMARY KEY needed)", "[merge]") {
	// End-to-end check of the connector's own generated SQL against in-memory
	// DuckDB: it upserts correctly and stays idempotent on replay, without any
	// key constraint on the target.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger, PrimaryKeyMode::NotNull);

	std::vector<column_def> cols = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	    column_def {.name = "v", .type = duckdb::LogicalTypeId::VARCHAR},
	};
	std::vector<const column_def*> columns_pk;
	std::vector<const column_def*> columns_regular;
	find_primary_keys(cols, columns_pk, &columns_regular);

	const table_def table {"memory", "main", "t"};
	ok(con, "CREATE TABLE \"memory\".\"main\".\"t\" (\"id\" INTEGER NOT NULL, \"v\" VARCHAR)");
	ok(con, "CREATE TABLE staging (\"id\" INTEGER, \"v\" VARCHAR)");
	ok(con, "INSERT INTO staging VALUES (1, 'a'), (2, 'b')");

	generator.upsert(con, table, "staging", columns_pk, columns_regular);
	REQUIRE(row_count(con, "\"memory\".\"main\".\"t\"") == 2);

	// Replay the same staging rows: matched, not duplicated.
	generator.upsert(con, table, "staging", columns_pk, columns_regular);
	REQUIRE(row_count(con, "\"memory\".\"main\".\"t\"") == 2);

	// New value for an existing key updates in place.
	ok(con, "DELETE FROM staging");
	ok(con, "INSERT INTO staging VALUES (1, 'a-updated'), (3, 'c')");
	generator.upsert(con, table, "staging", columns_pk, columns_regular);
	REQUIRE(row_count(con, "\"memory\".\"main\".\"t\"") == 3);
	REQUIRE(scalar(con, "SELECT v FROM \"memory\".\"main\".\"t\" WHERE id = 1") == "a-updated");
}
