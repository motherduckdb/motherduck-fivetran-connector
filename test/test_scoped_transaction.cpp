#include "duckdb.hpp"
#include "md_logging.hpp"
#include "scoped_transaction.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace {

std::int64_t count_rows(duckdb::Connection& con) {
	const auto res = con.Query("SELECT count(*) FROM t");
	REQUIRE_FALSE(res->HasError());
	return res->GetValue(0, 0).GetValue<std::int64_t>();
}

} // namespace

TEST_CASE("ScopedTransaction", "[scoped_transaction]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	REQUIRE_FALSE(con.Query("CREATE TABLE t (i INTEGER)")->HasError());

	SECTION("committed work is kept") {
		{
			ScopedTransaction transaction(con);
			REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (1)")->HasError());
			transaction.Commit();
			REQUIRE_FALSE(con.HasActiveTransaction());
		}
		REQUIRE(count_rows(con) == 1);
	}

	SECTION("uncommitted work is rolled back") {
		{
			ScopedTransaction transaction(con);
			REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (1)")->HasError());
		}
		REQUIRE_FALSE(con.HasActiveTransaction());
		REQUIRE(count_rows(con) == 0);
	}

	SECTION("a transaction owned by an outer scope is left alone") {
		con.BeginTransaction();
		{
			ScopedTransaction transaction(con);
			REQUIRE_FALSE(con.Query("INSERT INTO t VALUES (1)")->HasError());
			transaction.Commit();
		}
		REQUIRE(con.HasActiveTransaction());
		con.Commit();
		REQUIRE(count_rows(con) == 1);
	}
}

// The schema creation retries on catalog write-write conflicts, which parallel syncs into the same schema run
// into. A transaction started further out would silently disable those retries, so the generator rejects it.
TEST_CASE("Schema creation refuses to run inside a transaction", "[scoped_transaction]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	auto logger = mdlog::Logger::CreateNopLogger();
	const MdSqlGenerator generator(logger);

	REQUIRE_NOTHROW(generator.create_schema_if_not_exists_with_retries(con, "memory", "outside_transaction"));

	ScopedTransaction transaction(con);
	REQUIRE_THROWS_AS(generator.create_schema_if_not_exists_with_retries(con, "memory", "inside_transaction"),
	                  std::logic_error);
}
