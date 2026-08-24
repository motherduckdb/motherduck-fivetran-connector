#include "duckdb.hpp"
#include "integration/common.hpp"
#include "md_error.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <string>

TEST_CASE("throw_recoverable_error_if_oom converts DuckDB out-of-memory errors into a RecoverableError",
          "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='1MB'"));

	const auto result = con.Query("CREATE TABLE t AS SELECT i, repeat('x', 1000000) AS v FROM range(100) t(i)");
	REQUIRE(result->HasError());
	REQUIRE(result->GetErrorObject().Type() == duckdb::ExceptionType::OUT_OF_MEMORY);

	REQUIRE_THROWS_AS(throw_recoverable_error_if_oom(result->GetErrorObject()), md_error::RecoverableError);
	REQUIRE_THROWS_WITH(throw_recoverable_error_if_oom(result->GetErrorObject()),
	                    Catch::Matchers::ContainsSubstring("upgrade to a larger instance size"));
}

TEST_CASE("throw_recoverable_error_if_oom is a no-op for non-OOM errors", "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	const auto result = con.Query("SELECT * FROM nonexistent_table");
	REQUIRE(result->HasError());
	REQUIRE(result->GetErrorObject().Type() != duckdb::ExceptionType::OUT_OF_MEMORY);

	REQUIRE_NOTHROW(throw_recoverable_error_if_oom(result->GetErrorObject()));
}

TEST_CASE("throw_if_query_error is a no-op for a successful result", "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	const auto result = con.Query("SELECT 1");
	REQUIRE_NOTHROW(throw_if_query_error(*result, "should not be thrown"));
}

TEST_CASE("throw_if_query_error wraps a regular error in a prefixed std::runtime_error", "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	const auto result = con.Query("SELECT * FROM nonexistent_table");
	REQUIRE(result->HasError());

	REQUIRE_THROWS_AS(throw_if_query_error(*result, "Could not query table"), std::runtime_error);
	REQUIRE_THROWS_WITH(throw_if_query_error(*result, "Could not query table"),
	                    Catch::Matchers::ContainsSubstring("Could not query table"));
}

TEST_CASE("throw_if_query_error converts an out-of-memory error into a RecoverableError", "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='1MB'"));

	const auto result = con.Query("CREATE TABLE t AS SELECT i, repeat('x', 1000000) AS v FROM range(100) t(i)");
	REQUIRE(result->HasError());
	REQUIRE(result->GetErrorObject().Type() == duckdb::ExceptionType::OUT_OF_MEMORY);

	REQUIRE_THROWS_AS(throw_if_query_error(*result, "Could not create table"), md_error::RecoverableError);
}
