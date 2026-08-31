#include "duckdb.hpp"
#include "integration/common.hpp"
#include "md_error.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <stdexcept>
#include <string>

namespace {
// Forces a real DuckDB out-of-memory error rather than simulating one.
duckdb::unique_ptr<duckdb::MaterializedQueryResult> out_of_memory_result(duckdb::Connection& con) {
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='1MB'"));
	auto result = con.Query("CREATE TABLE t AS SELECT i, repeat('x', 1000000) AS v FROM range(100) t(i)");
	REQUIRE(result->HasError());
	REQUIRE(result->GetErrorObject().Type() == duckdb::ExceptionType::OUT_OF_MEMORY);
	return result;
}
} // namespace

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
	// The prefix and DuckDB's own error type are both kept, which integration tests assert on.
	REQUIRE_THROWS_WITH(throw_if_query_error(*result, "Could not query table"),
	                    Catch::Matchers::ContainsSubstring("Could not query table: ") &&
	                        Catch::Matchers::ContainsSubstring("Catalog Error:"));
}

TEST_CASE("throw_if_query_error turns an out-of-memory error into a RecoverableError", "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	const auto result = out_of_memory_result(con);

	REQUIRE_THROWS_AS(throw_if_query_error(*result, "Could not create table"), md_error::RecoverableError);
	REQUIRE_THROWS_WITH(throw_if_query_error(*result, "Could not create table"),
	                    Catch::Matchers::ContainsSubstring("larger Duckling instance size"));
}

TEST_CASE("throw_if_query_error handles a prepared statement", "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	const auto statement = con.Prepare("SELECT nonexistent_column");
	REQUIRE(statement->HasError());
	REQUIRE_THROWS_AS(throw_if_query_error(*statement, "at bind step"), std::runtime_error);
}
