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

TEST_CASE("AlterTable recreate defaults a new key column of every Fivetran type", "[alter]") {
	// Adding a key column to a populated table needs a default the column type accepts. Check that there is a usable
	// default for each column type.
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
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER PRIMARY KEY)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1)"));

		column_def new_key {.name = "new_key", .type = duckdb_type, .primary_key = true};
		if (duckdb_type == duckdb::LogicalTypeId::DECIMAL) {
			new_key.width = DECIMAL_DEFAULT_WIDTH;
			new_key.scale = DECIMAL_DEFAULT_SCALE;
		}
		const std::vector<column_def> requested = {
		    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true}, new_key};
		generator.alter_table(con, table, requested, /*drop_columns=*/false);

		// The existing row survives the recreate, with the default filled in for the new key column.
		auto res = con.Query("SELECT COUNT(*) FROM t WHERE \"new_key\" IS NOT NULL");
		REQUIRE_NO_FAIL(res);
		REQUIRE(res->GetValue(0, 0).GetValue<int64_t>() == 1);
	}
}
