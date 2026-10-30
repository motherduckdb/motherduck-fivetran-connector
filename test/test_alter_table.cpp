#include "duckdb.hpp"
#include "integration/common.hpp"
#include "md_error.hpp"
#include "md_logging.hpp"
#include "schema_types.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
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

TEST_CASE("AlterTable raises a task when an existing column with NULLs joins the primary key", "[alter]") {
	// The column can only become NOT NULL if every row has a value, which only the
	// source can fix, so the connector surfaces a task instead of a raw constraint
	// error. The failed alter leaves the table untouched.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::NotNull;

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER NOT NULL, \"region\" VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO " + QUALIFIED + " VALUES (1, 'us'), (2, NULL)")); // region has a NULL

	// Promote the existing, partly-NULL "region" column into the key.
	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER),
	                                     key_col("region", duckdb::LogicalTypeId::VARCHAR)};

	// A RecoverableError is what the server turns into a Fivetran task.
	std::string task_message;
	try {
		generator.alter_table(con, TABLE, requested, /*drop_columns=*/false, pk_mode);
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

TEST_CASE("AlterTable recreate raises a task when an existing column with NULLs joins the primary key", "[alter]") {
	// Same as above, but the table has an enforced PRIMARY KEY, so the key change recreates the table in both modes.
	const bool strict = GENERATE(false, true);
	CAPTURE(strict);
	const PrimaryKeyMode pk_mode = strict ? PrimaryKeyMode::Strict : PrimaryKeyMode::NotNull;

	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE " + QUALIFIED + " (\"id\" INTEGER, \"region\" VARCHAR, PRIMARY KEY (\"id\"))"));
	// The ids are unique, so the strict-mode duplicate check alone would not catch this.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO " + QUALIFIED + " VALUES (1, 'us'), (2, NULL)"));

	std::vector<column_def> requested = {key_col("id", duckdb::LogicalTypeId::INTEGER),
	                                     key_col("region", duckdb::LogicalTypeId::VARCHAR)};

	std::string task_message;
	try {
		generator.alter_table(con, TABLE, requested, /*drop_columns=*/false, pk_mode);
		FAIL("expected a RecoverableError");
	} catch (const md_error::RecoverableError& e) {
		task_message = e.what();
	}
	REQUIRE_THAT(task_message, Catch::Matchers::ContainsSubstring("contains NULL values"));
	REQUIRE_THAT(task_message, Catch::Matchers::ContainsSubstring("region"));

	// The table is untouched: the PRIMARY KEY is still there, region is still nullable and both rows survive.
	REQUIRE(primary_key_count(con) == 1);
	REQUIRE_FALSE(column_is_not_null(con, "region"));
	REQUIRE(row_count(con, QUALIFIED) == 2);
}

TEST_CASE("DuckLake rejects SET NOT NULL on a newly added column", "[alter][ducklake]") {
	// Documents a DuckLake limitation the NotNull key mode runs into: adding a
	// column and marking it NOT NULL -- what alter_table_in_place does for a newly
	// added key column -- is refused, because DuckLake validates the constraint
	// against column stats, which a newly added column does not have. Skipped
	// where the ducklake extension is not already available (no download here).
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	if (con.Query("LOAD ducklake")->HasError()) {
		SKIP("ducklake extension is not available in this environment");
	}

	const std::string dir_name = "ducklake_notnull_" + duckdb::StringUtil::GenerateRandomName(8);
	const auto dir = std::filesystem::temp_directory_path() / dir_name;
	std::filesystem::create_directories(dir);
	REQUIRE_NO_FAIL(con.Query("ATTACH 'ducklake:" + (dir / "catalog.ducklake").string() + "' AS lake (DATA_PATH '" +
	                          (dir / "data").string() + "/')"));

	// NOT NULL at CREATE works
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE lake.t (\"id\" INTEGER NOT NULL, \"v\" VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO lake.t VALUES (1, 'a')"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE lake.t ADD COLUMN \"region\" VARCHAR DEFAULT ''"));
	auto res = con.Query("ALTER TABLE lake.t ALTER COLUMN \"region\" SET NOT NULL");
	REQUIRE(res->HasError());
	REQUIRE_THAT(res->GetError(), Catch::Matchers::ContainsSubstring("SET NOT NULL"));
	con.Query("ROLLBACK");

	// Committing the ADD COLUMN first does not help: the column still has no stats.
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE lake.t ADD COLUMN \"region2\" VARCHAR DEFAULT ''"));
	res = con.Query("ALTER TABLE lake.t ALTER COLUMN \"region2\" SET NOT NULL");
	REQUIRE(res->HasError());
	REQUIRE_THAT(res->GetError(), Catch::Matchers::ContainsSubstring("no column stats are available"));

	con.Query("DETACH lake");
	std::filesystem::remove_all(dir);
}

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
	generator.alter_table(con, table, requested, /*drop_columns=*/false, PrimaryKeyMode::Strict);

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
	generator.alter_table(con, table, requested, /*drop_columns=*/true, PrimaryKeyMode::Strict);

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
	generator.alter_table(con, table, requested, /*drop_columns=*/false, PrimaryKeyMode::Strict);

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

TEST_CASE("AlterTable defaults a new key column of every Fivetran type", "[alter]") {
	// Adding a key column to a populated table needs a default the column type accepts. Check that there is a usable
	// default for each column type, both when the table is recreated (it has a PRIMARY KEY) and when the column is
	// added in place and set NOT NULL in the same transaction (it has none).
	const bool table_has_pk = GENERATE(false, true);
	CAPTURE(table_has_pk);
	const PrimaryKeyMode pk_mode = table_has_pk ? PrimaryKeyMode::Strict : PrimaryKeyMode::NotNull;

	const auto* fivetran_types = fivetran_sdk::v2::DataType_descriptor();
	for (int i = 0; i < fivetran_types->value_count(); i++) {
		const auto fivetran_type = static_cast<fivetran_sdk::v2::DataType>(fivetran_types->value(i)->number());
		if (fivetran_type == fivetran_sdk::v2::UNSPECIFIED) {
			continue;
		}
		INFO("Fivetran type " << fivetran_types->value(i)->name());

		const auto duckdb_type = get_duckdb_type(fivetran_type);
		REQUIRE(duckdb_type != duckdb::LogicalTypeId::INVALID);

		duckdb::DuckDB db(nullptr);
		duckdb::Connection con(db);
		auto logger = mdlog::Logger::CreateNopLogger();
		MdSqlGenerator generator(logger);

		const table_def table {"memory", "main", "t"};
		REQUIRE_NO_FAIL(con.Query(table_has_pk ? "CREATE TABLE t (id INTEGER PRIMARY KEY)"
		                                       : "CREATE TABLE t (id INTEGER NOT NULL)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1)"));

		column_def new_key {.name = "new_key", .type = duckdb_type, .primary_key = true};
		if (duckdb_type == duckdb::LogicalTypeId::DECIMAL) {
			new_key.width = DECIMAL_DEFAULT_WIDTH;
			new_key.scale = DECIMAL_DEFAULT_SCALE;
		}
		const std::vector<column_def> requested = {
		    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true}, new_key};
		generator.alter_table(con, table, requested, /*drop_columns=*/false, pk_mode);

		// The existing row survives, with the default filled in for the new key column.
		auto res = con.Query("SELECT COUNT(*) FROM t WHERE \"new_key\" IS NOT NULL");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->GetValue(0, 0).GetValue<int64_t>() == 1);
		REQUIRE(column_is_not_null(con, "new_key"));
	}
}
