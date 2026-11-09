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
	// A column the request no longer contains must not stay part of the primary key. WriteBatch no longer writes it,
	// so its NOT NULL would reject every new row.
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
	// default for each column type, both in strict mode, where the table is recreated, and in NotNull mode, where the
	// column is added in place and set NOT NULL in the same transaction.
	const bool is_strict_pk_mode = GENERATE(false, true);
	CAPTURE(is_strict_pk_mode);
	const PrimaryKeyMode pk_mode = is_strict_pk_mode ? PrimaryKeyMode::Strict : PrimaryKeyMode::NotNull;

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
		REQUIRE_NO_FAIL(con.Query(is_strict_pk_mode ? "CREATE TABLE t (id INTEGER PRIMARY KEY)"
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
		REQUIRE(primary_key_names(generator.describe_table(con, table)) == std::vector<std::string> {"id", "new_key"});
	}
}

TEST_CASE("AlterTable in strict mode recreates the table when dropping a key column", "[alter]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER, b INTEGER, PRIMARY KEY (id, b))"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 1)"));

	// Drop b (part of the old PK). The key becomes just id. DuckDB cannot drop a column the PRIMARY KEY depends on.
	std::vector requested = {column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true}};
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::Strict;
	generator.alter_table(con, table, requested, /*drop_columns=*/true, pk_mode);

	// b is gone, the row survives and the PRIMARY KEY now covers id only.
	const auto columns = generator.describe_table(con, table);
	REQUIRE(columns.size() == 1);
	REQUIRE(primary_key_names(columns) == std::vector<std::string> {"id"});
	auto res = con.Query("SELECT id FROM t");
	REQUIRE_NO_FAIL(res);
	REQUIRE(res->RowCount() == 1);
	REQUIRE_THAT(con.Query("INSERT INTO t VALUES (1)")->GetError(),
	             Catch::Matchers::ContainsSubstring("Duplicate key \"id: 1\" violates primary key constraint"));
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

	// The new key column is NOT NULL and the existing row is preserved with its default.
	REQUIRE(primary_key_names(generator.describe_table(con, table)) == std::vector<std::string> {"id", "region"});
	auto res = con.Query("SELECT id, v, region FROM t");
	REQUIRE_NO_FAIL(res);
	REQUIRE(res->RowCount() == 1);
	check_row(res, 0, {duckdb::Value::INTEGER(1), "a", ""});
	// Still no PRIMARY KEY (we never introduce one in NotNull mode), so a duplicate key inserts fine.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'b', '')"));
}

TEST_CASE("AlterTable in NotNull mode adds a key column, changes a type and extends the key in one request",
          "[alter]") {
	// All statements run in place in one transaction. DuckDB rejects SET NOT NULL after an UPDATE on the same table in
	// the same transaction ("Cannot create index with outstanding updates"), so none of them may turn into one.
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, region VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 10, 'us'), (2, 20, 'eu')"));
	auto table_oid_before = con.Query("SELECT table_oid FROM duckdb_tables() WHERE table_name = 't'");
	REQUIRE_NO_FAIL(table_oid_before);

	// v changes its type, region joins the key and k is a new key column.
	std::vector requested = {column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
	                         column_def {.name = "v", .type = duckdb::LogicalTypeId::BIGINT},
	                         column_def {.name = "region", .type = duckdb::LogicalTypeId::VARCHAR, .primary_key = true},
	                         column_def {.name = "k", .type = duckdb::LogicalTypeId::VARCHAR, .primary_key = true}};
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::NotNull;
	generator.alter_table(con, table, requested, /*drop_columns=*/false, pk_mode);

	const auto columns = generator.describe_table(con, table);
	REQUIRE(primary_key_names(columns) == std::vector<std::string> {"id", "region", "k"});
	REQUIRE(columns[1].type == duckdb::LogicalTypeId::BIGINT);
	auto res = con.Query("SELECT id, v, region, k FROM t ORDER BY id");
	REQUIRE_NO_FAIL(res);
	REQUIRE(res->RowCount() == 2);
	check_row(res, 0, {duckdb::Value::INTEGER(1), duckdb::Value::BIGINT(10), "us", ""});
	check_row(res, 1, {duckdb::Value::INTEGER(2), duckdb::Value::BIGINT(20), "eu", ""});

	// A recreated table would have a new oid.
	auto table_oid_after = con.Query("SELECT table_oid FROM duckdb_tables() WHERE table_name = 't'");
	REQUIRE_NO_FAIL(table_oid_after);
	REQUIRE(table_oid_after->GetValue(0, 0) == table_oid_before->GetValue(0, 0));
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
	const PrimaryKeyMode pk_mode = PrimaryKeyMode::Strict;
	generator.alter_table(con, table, requested, /*drop_columns=*/false, pk_mode);

	// Strict mode re-adds an enforced PRIMARY KEY (now over the new key set), so a
	// duplicate on (id, region) is rejected.
	auto res = con.Query("SELECT id FROM t");
	REQUIRE_NO_FAIL(res);
	REQUIRE(res->RowCount() == 2);
	auto dup = con.Query("INSERT INTO t VALUES (1, 'c', '')"); // (1, '') already exists
	REQUIRE(dup->HasError());
	REQUIRE_THAT(dup->GetError(), Catch::Matchers::ContainsSubstring(
	                                  "Duplicate key \"id: 1, region: \" violates primary key constraint"));
}

TEST_CASE("AlterTable raises a task when an existing column with NULLs joins the primary key", "[alter]") {
	// The column can only become NOT NULL if every row has a value, which only the source can fix, so the connector
	// surfaces a task instead of a raw constraint error. In NotNull mode, the key changes in place and SET NOT NULL
	// fails. In strict mode, the table is recreated and the check before the recreate catches the NULL.
	const bool strict = GENERATE(false, true);
	CAPTURE(strict);
	const PrimaryKeyMode pk_mode = strict ? PrimaryKeyMode::Strict : PrimaryKeyMode::NotNull;

	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	MdSqlGenerator generator(logger);

	const table_def table {"memory", "main", "t"};
	REQUIRE_NO_FAIL(con.Query(strict ? "CREATE TABLE t (id INTEGER PRIMARY KEY, region VARCHAR)"
	                                 : "CREATE TABLE t (id INTEGER NOT NULL, region VARCHAR)"));
	// region has a NULL. The ids are unique, so in strict mode, the duplicate check alone would not catch this.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'us'), (2, NULL)"));

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

	// The table is untouched: region is still nullable, both rows survive and strict mode keeps its PRIMARY KEY.
	REQUIRE(primary_key_names(generator.describe_table(con, table)) == std::vector<std::string> {"id"});
	auto res = con.Query("SELECT id, region FROM t ORDER BY id");
	REQUIRE_NO_FAIL(res);
	REQUIRE(res->RowCount() == 2);
	check_row(res, 0, {duckdb::Value::INTEGER(1), "us"});
	check_row(res, 1, {duckdb::Value::INTEGER(2), duckdb::Value()});
	REQUIRE(con.Query("INSERT INTO t VALUES (1, 'eu')")->HasError() == strict);
}
