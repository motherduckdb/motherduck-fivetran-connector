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

	const table_def table {"memory", "main", "t"};
	// Table created in strict mode: enforced PRIMARY KEY on id.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'a')"));

	// Widen the key to (id, region) by adding a new key column.
	std::vector requested = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	    column_def {.name = "v", .type = duckdb::LogicalTypeId::VARCHAR},
	    column_def {.name = "region", .type = duckdb::LogicalTypeId::VARCHAR, .primary_key = true}};
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::NotNull;
	generator.alter_table(con, table, requested, /*drop_columns=*/false, pk_mode);

	// The strict-mode PK is gone (recreated without one) and the new key column is NOT NULL.
	REQUIRE(primary_key_count(con) == 0);
	REQUIRE(column_is_not_null(con, "region"));
	REQUIRE(column_is_not_null(con, "id"));
	REQUIRE(row_count(con, "t") == 1); // existing row preserved

	// Data non-unique on the OLD pk (id) but unique on the new key now inserts fine.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (2, 'x', 'us'), (2, 'y', 'eu')"));
	REQUIRE(row_count(con, "t") == 3);
}

TEST_CASE("AlterTable in NotNull mode recreates a strict-mode table when dropping a key column", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	// Table created in strict mode: composite PRIMARY KEY (id, b).
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER, b INTEGER, PRIMARY KEY (id, b))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 1)"));

	// Drop b (part of the old PK). The key becomes just id.
	std::vector requested = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true}};
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::NotNull;
	generator.alter_table(con, table, requested, /*drop_columns=*/true, pk_mode);

	//b is gone, the PK is gone, and id column is NOT NULL.
	REQUIRE(primary_key_count(con) == 0);
	REQUIRE(scalar(con, "SELECT count(*) FROM duckdb_columns() WHERE table_name = 't' AND column_name = 'b'") == "0");
	REQUIRE(column_is_not_null(con, "id"));
	REQUIRE(row_count(con, "t") == 1);
}

TEST_CASE("AlterTable changes the key in place for a NOT NULL table (no PRIMARY KEY, no recreate needed)", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	// Table created in NotNull mode
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'a')"));

	std::vector requested = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	    column_def {.name = "v", .type = duckdb::LogicalTypeId::VARCHAR},
	    column_def {.name = "region", .type = duckdb::LogicalTypeId::VARCHAR, .primary_key = true}};
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::NotNull;
	generator.alter_table(con, table, requested, /*drop_columns=*/false, pk_mode);

	// Still no PRIMARY KEY (we never introduce one in NotNull mode); the new key
	// column is NOT NULL; the existing row is preserved in place.
	REQUIRE(primary_key_count(con) == 0);
	REQUIRE(column_is_not_null(con, "region"));
	REQUIRE(row_count(con, "t") == 1);
	// Non-unique on id but unique on (id, region) inserts fine.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (2, 'x', 'us'), (2, 'y', 'eu')"));
	REQUIRE(row_count(con, "t") == 3);
}

TEST_CASE("AlterTable in strict mode rebuilds the PRIMARY KEY over the new key", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER, v VARCHAR, PRIMARY KEY (id))"));
	// unique ids -> new key stays unique
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'a'), (2, 'b')"));

	std::vector requested = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	    column_def {.name = "v", .type = duckdb::LogicalTypeId::VARCHAR},
	    column_def {.name = "region", .type = duckdb::LogicalTypeId::VARCHAR, .primary_key = true}};
	generator.alter_table(con, table, requested, /*drop_columns=*/false, pk_mode);

	// Strict mode re-adds an enforced PRIMARY KEY (now over the new key set), so a
	// duplicate on (id, region) is rejected.
	REQUIRE(primary_key_count(con) == 1);
	REQUIRE(row_count(con, "t") == 2);
	auto dup = con.Query("INSERT INTO t VALUES (1, 'c', '')"); // (1, '') already exists
	REQUIRE(dup->HasError());
	REQUIRE_THAT(dup->GetError(), Catch::Matchers::ContainsSubstring(
	                                  "Duplicate key \"id: 1, region: \" violates primary key constraint"));
}

TEST_CASE("AlterTable in strict mode adds the PRIMARY KEY in place to a NotNull-mode table", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'a'), (2, 'b')"));
	const std::string table_oid_query = "SELECT table_oid FROM duckdb_tables() WHERE table_name = 't'";
	const auto table_oid = scalar(con, table_oid_query);

	// The key stays the same, the request only adds a regular column.
	std::vector requested = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	    column_def {.name = "v", .type = duckdb::LogicalTypeId::VARCHAR},
	    column_def {.name = "w", .type = duckdb::LogicalTypeId::VARCHAR}};
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::Strict;
	generator.alter_table(con, table, requested, /*drop_columns=*/false, pk_mode);

	REQUIRE(primary_key_count(con) == 1);
	REQUIRE(row_count(con, "t") == 2);
	REQUIRE_FALSE(column_is_not_null(con, "w"));
	// A recreated table would have a new oid.
	REQUIRE(scalar(con, table_oid_query) == table_oid);
}

TEST_CASE("AlterTable in strict mode changes the key of a NotNull-mode table in place", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR NOT NULL)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'a'), (2, 'b')"));
	const std::string table_oid_query = "SELECT table_oid FROM duckdb_tables() WHERE table_name = 't'";
	const auto table_oid = scalar(con, table_oid_query);

	// v leaves the key and region joins it.
	std::vector requested = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	    column_def {.name = "v", .type = duckdb::LogicalTypeId::VARCHAR},
	    column_def {.name = "region", .type = duckdb::LogicalTypeId::VARCHAR, .primary_key = true}};
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::Strict;
	generator.alter_table(con, table, requested, /*drop_columns=*/false, pk_mode);

	REQUIRE(primary_key_names(generator.describe_table(con, table)) == std::vector<std::string> {"id", "region"});
	REQUIRE(primary_key_count(con) == 1);
	REQUIRE(row_count(con, "t") == 2);
	REQUIRE(scalar(con, table_oid_query) == table_oid);
	// The PRIMARY KEY covers the new key, so a duplicate on (id, region) is rejected.
	REQUIRE(con.Query("INSERT INTO t VALUES (1, NULL, '')")->HasError());
}

TEST_CASE("AlterTable in strict mode raises a task when a NotNull-mode table contains duplicate keys", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'a'), (1, 'b')"));

	// The new column w either leaves the key unchanged or joins it. Both rows get the same default for w, so the
	// key stays duplicated either way.
	const bool w_is_key = GENERATE(false, true);
	CAPTURE(w_is_key);
	std::vector requested = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	    column_def {.name = "v", .type = duckdb::LogicalTypeId::VARCHAR},
	    column_def {.name = "w", .type = duckdb::LogicalTypeId::VARCHAR, .primary_key = w_is_key}};

	std::string task_message;
	try {
		const PrimaryKeyMode pk_mode = PrimaryKeyMode::Strict;
		generator.alter_table(con, table, requested, /*drop_columns=*/false, pk_mode);
		FAIL("expected a RecoverableError");
	} catch (const md_error::RecoverableError& e) {
		task_message = e.what();
	}
	REQUIRE_THAT(task_message, Catch::Matchers::ContainsSubstring("contains duplicate primary keys"));

	// Rolled back: no PRIMARY KEY, no new column, both rows survive.
	REQUIRE(primary_key_count(con) == 0);
	REQUIRE(scalar(con, "SELECT count(*) FROM duckdb_columns() WHERE table_name = 't' AND column_name = 'w'") == "0");
	REQUIRE(row_count(con, "t") == 2);
}

TEST_CASE("AlterTable raises a task when an existing column with NULLs joins the primary key", "[alter]") {
	// The column can only become NOT NULL if every row has a value, which only the
	// source can fix, so the connector surfaces a task instead of a raw constraint
	// error. The failed alter leaves the table untouched.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::NotNull;

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER NOT NULL, region VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'us'), (2, NULL)")); // region has a NULL

	// Promote the existing, partly-NULL "region" column into the key.
	const std::vector<column_def> requested = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	    column_def {.name = "region", .type = duckdb::LogicalTypeId::VARCHAR, .primary_key = true}};

	// A RecoverableError is what the server turns into a Fivetran task.
	std::string task_message;
	try {
		generator.alter_table(con, table, requested, /*drop_columns=*/false, pk_mode);
		FAIL("expected a RecoverableError");
	} catch (const md_error::RecoverableError& e) {
		task_message = e.what();
	}
	REQUIRE_THAT(task_message, Catch::Matchers::ContainsSubstring("contains NULL values"));
	REQUIRE_THAT(task_message, Catch::Matchers::ContainsSubstring("region"));

	// Rolled back: region is still nullable, i.e. not a key column, and both rows survive.
	REQUIRE(primary_key_names(generator.describe_table(con, table)) == std::vector<std::string> {"id"});
	auto res = con.Query("SELECT id, region FROM t ORDER BY id");
	REQUIRE_NO_FAIL(res);
	REQUIRE(res->RowCount() == 2);
	check_row(res, 0, {duckdb::Value::INTEGER(1), "us"});
	check_row(res, 1, {duckdb::Value::INTEGER(2), duckdb::Value()});
}

TEST_CASE("AlterTable recreate raises a task when an existing column with NULLs joins the primary key", "[alter]") {
	// Same as above, but the table has an enforced PRIMARY KEY, so the key change recreates the table in both modes.
	const bool strict = GENERATE(false, true);
	CAPTURE(strict);
	const PrimaryKeyMode pk_mode = strict ? PrimaryKeyMode::Strict : PrimaryKeyMode::NotNull;

	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER PRIMARY KEY, region VARCHAR)"));
	// The ids are unique, so the strict-mode duplicate check alone would not catch this.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'us'), (2, NULL)"));

	const std::vector<column_def> requested = {
	    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	    column_def {.name = "region", .type = duckdb::LogicalTypeId::VARCHAR, .primary_key = true}};

	std::string task_message;
	try {
		generator.alter_table(con, table, requested, /*drop_columns=*/false, pk_mode);
		FAIL("expected a RecoverableError");
	} catch (const md_error::RecoverableError& e) {
		task_message = e.what();
	}
	REQUIRE_THAT(task_message, Catch::Matchers::ContainsSubstring("contains NULL values"));
	REQUIRE_THAT(task_message, Catch::Matchers::ContainsSubstring("region"));

	// The table is untouched: region is still nullable, both rows survive and the PRIMARY KEY still rejects a duplicate
	// id.
	REQUIRE(primary_key_names(generator.describe_table(con, table)) == std::vector<std::string> {"id"});
	auto res = con.Query("SELECT id, region FROM t ORDER BY id");
	REQUIRE_NO_FAIL(res);
	REQUIRE(res->RowCount() == 2);
	check_row(res, 0, {duckdb::Value::INTEGER(1), "us"});
	check_row(res, 1, {duckdb::Value::INTEGER(2), duckdb::Value()});
	REQUIRE(con.Query("INSERT INTO t VALUES (1, 'eu')")->HasError());
}
