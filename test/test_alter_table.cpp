// Unit tests for MdSqlGenerator::alter_table's handling of tables that still
// carry an enforced PRIMARY KEY -- i.e. tables created before the connector
// switched to NOT NULL keys. A key change on such a table must recreate it,
// because an enforced PK cannot be altered or dropped in place. These run
// entirely against in-memory DuckDB (no MotherDuck).

#include "duckdb.hpp"
#include "md_error.hpp"
#include "md_logging.hpp"
#include "schema_types.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <filesystem>
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

} // namespace

TEST_CASE("AlterTable recreates a legacy PRIMARY KEY table when widening the key (NotNull mode)", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger, PrimaryKeyMode::NotNull);

	// Legacy table from the old connector: enforced PRIMARY KEY on id.
	ok(con, "CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER, \"v\" VARCHAR, PRIMARY KEY (\"id\"))");
	ok(con, "INSERT INTO " + QUALIFIED + " VALUES (1, 'a')");

	// Widen the key to (id, region) by adding a new key column.
	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER),
	                                     col("v", duckdb::LogicalTypeId::VARCHAR),
	                                     key_col("region", duckdb::LogicalTypeId::VARCHAR)};
	generator.alter_table(con, TABLE, requested, /*drop_columns=*/false);

	// The legacy PK is gone (recreated without one) and the new key column is NOT NULL.
	REQUIRE(primary_key_count(con) == 0);
	REQUIRE(column_is_not_null(con, "region"));
	REQUIRE(column_is_not_null(con, "id"));
	REQUIRE(row_count(con, QUALIFIED) == 1); // existing row preserved

	// Data non-unique on the OLD pk (id) but unique on the new key now inserts fine.
	ok(con, "INSERT INTO " + QUALIFIED + " VALUES (2, 'x', 'us'), (2, 'y', 'eu')");
	REQUIRE(row_count(con, QUALIFIED) == 3);
}

TEST_CASE("AlterTable recreates a legacy PRIMARY KEY table when dropping a key column (NotNull mode)", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger, PrimaryKeyMode::NotNull);

	// Legacy composite PRIMARY KEY (id, b).
	ok(con, "CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER, \"b\" INTEGER, PRIMARY KEY (\"id\", \"b\"))");
	ok(con, "INSERT INTO " + QUALIFIED + " VALUES (1, 1)");

	// Drop b (part of the old PK); the key becomes just id.
	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER)};
	generator.alter_table(con, TABLE, requested, /*drop_columns=*/true);

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
	MdSqlGenerator generator(logger, PrimaryKeyMode::NotNull);

	// A table created by the new connector: NOT NULL key, no PRIMARY KEY.
	ok(con, "CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER NOT NULL, \"v\" VARCHAR)");
	ok(con, "INSERT INTO " + QUALIFIED + " VALUES (1, 'a')");

	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER),
	                                     col("v", duckdb::LogicalTypeId::VARCHAR),
	                                     key_col("region", duckdb::LogicalTypeId::VARCHAR)};
	generator.alter_table(con, TABLE, requested, /*drop_columns=*/false);

	// Still no PRIMARY KEY (we never introduce one in NotNull mode); the new key
	// column is NOT NULL; the existing row is preserved in place.
	REQUIRE(primary_key_count(con) == 0);
	REQUIRE(column_is_not_null(con, "region"));
	REQUIRE(row_count(con, QUALIFIED) == 1);
	// Non-unique on id but unique on (id, region) inserts fine.
	ok(con, "INSERT INTO " + QUALIFIED + " VALUES (2, 'x', 'us'), (2, 'y', 'eu')");
	REQUIRE(row_count(con, QUALIFIED) == 3);
}

TEST_CASE("AlterTable in strict mode rebuilds the PRIMARY KEY on a legacy table's new key", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger, PrimaryKeyMode::Strict);

	ok(con, "CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER, \"v\" VARCHAR, PRIMARY KEY (\"id\"))");
	ok(con, "INSERT INTO " + QUALIFIED + " VALUES (1, 'a'), (2, 'b')"); // unique ids -> new key stays unique

	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER),
	                                     col("v", duckdb::LogicalTypeId::VARCHAR),
	                                     key_col("region", duckdb::LogicalTypeId::VARCHAR)};
	generator.alter_table(con, TABLE, requested, /*drop_columns=*/false);

	// Strict mode re-adds an enforced PRIMARY KEY (now over the new key set), so a
	// duplicate on (id, region) is rejected.
	REQUIRE(primary_key_count(con) == 1);
	REQUIRE(row_count(con, QUALIFIED) == 2);
	auto dup = con.Query("INSERT INTO " + QUALIFIED + " VALUES (1, 'c', '')"); // (1, '') already exists
	REQUIRE(dup->HasError());
}

TEST_CASE("AlterTable raises a task when an existing column with NULLs joins the primary key", "[alter]") {
	// The column can only become NOT NULL if every row has a value, which only the
	// source can fix, so the connector surfaces a task instead of a raw constraint
	// error. The failed alter leaves the table untouched.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger, PrimaryKeyMode::NotNull);

	ok(con, "CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER NOT NULL, \"region\" VARCHAR)");
	ok(con, "INSERT INTO " + QUALIFIED + " VALUES (1, 'us'), (2, NULL)"); // region has a NULL

	// Promote the existing, partly-NULL "region" column into the key.
	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER),
	                                     key_col("region", duckdb::LogicalTypeId::VARCHAR)};

	// A RecoverableError is what the server turns into a Fivetran task.
	std::string task_message;
	try {
		generator.alter_table(con, TABLE, requested, /*drop_columns=*/false);
		FAIL("expected a RecoverableError");
	} catch (const md_error::RecoverableError& e) {
		task_message = e.what();
	}
	REQUIRE_THAT(task_message, Catch::Matchers::ContainsSubstring("contains NULL values"));
	REQUIRE_THAT(task_message, Catch::Matchers::ContainsSubstring("region"));

	// Rolled back: region is still nullable and both rows survive.
	REQUIRE_FALSE(column_is_not_null(con, "region"));
	REQUIRE(row_count(con, QUALIFIED) == 2);
}

TEST_CASE("DuckLake rejects SET NOT NULL on a table with transaction-local changes", "[alter][ducklake]") {
	// Documents a DuckLake limitation the NotNull key mode runs into: adding a
	// column and marking it NOT NULL inside one transaction -- what
	// alter_table_in_place does for a newly added key column -- is refused, because
	// DuckLake needs committed column stats to validate the constraint. Skipped
	// where the ducklake extension is not already available (no download here).
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	if (con.Query("LOAD ducklake")->HasError()) {
		SKIP("ducklake extension is not available in this environment");
	}

	const auto dir = std::filesystem::temp_directory_path() /
	                 ("ducklake_notnull_" + duckdb::StringUtil::GenerateRandomName(8));
	std::filesystem::create_directories(dir);
	ok(con, "ATTACH 'ducklake:" + (dir / "catalog.ducklake").string() + "' AS lake (DATA_PATH '" +
	            (dir / "data").string() + "/')");

	ok(con, "CREATE TABLE lake.t (\"id\" INTEGER NOT NULL, \"v\" VARCHAR)"); // NOT NULL at CREATE works
	ok(con, "INSERT INTO lake.t VALUES (1, 'a')");

	ok(con, "BEGIN TRANSACTION");
	ok(con, "ALTER TABLE lake.t ADD COLUMN \"region\" VARCHAR DEFAULT ''");
	auto res = con.Query("ALTER TABLE lake.t ALTER COLUMN \"region\" SET NOT NULL");
	REQUIRE(res->HasError());
	REQUIRE_THAT(res->GetError(), Catch::Matchers::ContainsSubstring("SET NOT NULL"));
	con.Query("ROLLBACK");

	// Committing the ADD COLUMN first makes the same SET NOT NULL succeed, which is
	// the shape a DuckLake-compatible implementation needs to use.
	ok(con, "ALTER TABLE lake.t ADD COLUMN \"region2\" VARCHAR DEFAULT ''");
	ok(con, "ALTER TABLE lake.t ALTER COLUMN \"region2\" SET NOT NULL");

	con.Query("DETACH lake");
	std::filesystem::remove_all(dir);
}
