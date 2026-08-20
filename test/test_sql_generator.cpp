#include "duckdb.hpp"
#include "integration/common.hpp"
#include "md_error.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <string>

TEST_CASE("throw_if_recoverable_oom_error converts DuckDB out-of-memory errors into a RecoverableError",
          "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='1MB'"));

	const auto result = con.Query("CREATE TABLE t AS SELECT i, repeat('x', 1000000) AS v FROM range(100) t(i)");
	REQUIRE(result->HasError());
	REQUIRE(result->GetErrorObject().Type() == duckdb::ExceptionType::OUT_OF_MEMORY);

	REQUIRE_THROWS_AS(throw_if_recoverable_oom_error(result->GetErrorObject()), md_error::RecoverableError);
	REQUIRE_THROWS_WITH(throw_if_recoverable_oom_error(result->GetErrorObject()),
	                    Catch::Matchers::ContainsSubstring("upgrade to a larger instance size"));
}

TEST_CASE("throw_if_recoverable_oom_error is a no-op for non-OOM errors", "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	const auto result = con.Query("SELECT * FROM nonexistent_table");
	REQUIRE(result->HasError());
	REQUIRE(result->GetErrorObject().Type() != duckdb::ExceptionType::OUT_OF_MEMORY);

	REQUIRE_NOTHROW(throw_if_recoverable_oom_error(result->GetErrorObject()));
}
