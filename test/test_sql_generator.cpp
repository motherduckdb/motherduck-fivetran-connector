#include "duckdb.hpp"
#include "integration/common.hpp"
#include "md_error.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <exception>
#include <new>
#include <stdexcept>
#include <string>

TEST_CASE("recoverable_oom_message returns an actionable message for DuckDB out-of-memory errors", "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET memory_limit=\'1MB\'"));

	const auto result = con.Query("CREATE TABLE t AS SELECT i, repeat(\'x\', 1000000) AS v FROM range(100) t(i)");
	REQUIRE(result->HasError());
	REQUIRE(result->GetErrorObject().Type() == duckdb::ExceptionType::OUT_OF_MEMORY);

	const auto message = recoverable_oom_message(result->GetErrorObject());
	REQUIRE(message.has_value());
	REQUIRE_THAT(*message, Catch::Matchers::ContainsSubstring("upgrade to a larger instance size"));
	// The original DuckDB message is kept for diagnostics.
	REQUIRE_THAT(*message, Catch::Matchers::ContainsSubstring(result->GetErrorObject().RawMessage()));
}

TEST_CASE("recoverable_oom_message returns nullopt for non-OOM errors", "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	const auto result = con.Query("SELECT * FROM nonexistent_table");
	REQUIRE(result->HasError());

	REQUIRE_FALSE(recoverable_oom_message(result->GetErrorObject()).has_value());
}

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

// The tests below cover the safety net in motherduck_destination_server.cpp, which handles an out-of-memory
// error that reaches an endpoint's generic catch arm without having gone through throw_if_query_error --
// for instance one thrown from inside DuckDB itself. The net's whole body is
// `recoverable_oom_message(duckdb::ErrorData(ex))`, which is what these exercise.

TEST_CASE("an out-of-memory error thrown by DuckDB is still recognized after being caught as a std::exception",
          "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='1MB'"));

	const auto result = con.Query("CREATE TABLE t AS SELECT i, repeat('x', 1000000) AS v FROM range(100) t(i)");
	REQUIRE(result->HasError());
	REQUIRE(result->GetErrorObject().Type() == duckdb::ExceptionType::OUT_OF_MEMORY);

	bool threw = false;
	try {
		// How DuckDB itself throws: the ExceptionType is JSON-encoded into what().
		result->GetErrorObject().Throw();
	} catch (const std::exception& ex) {
		threw = true;
		const auto message = recoverable_oom_message(duckdb::ErrorData(ex));
		REQUIRE(message.has_value());
		REQUIRE_THAT(*message, Catch::Matchers::ContainsSubstring("upgrade to a larger instance size"));
	}
	REQUIRE(threw);
}

TEST_CASE("the safety net leaves the exceptions thrown by throw_if_query_error untouched", "[sql_generator]") {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	const auto result = con.Query("SELECT * FROM nonexistent_table");
	REQUIRE(result->HasError());

	bool threw = false;
	try {
		throw_if_query_error(*result, "Could not query table");
	} catch (const std::exception& ex) {
		threw = true;
		// Not an OOM, so the net declines it and the endpoint falls through to its hard-failure path...
		REQUIRE_FALSE(recoverable_oom_message(duckdb::ErrorData(ex)).has_value());
		// ...with the readable message intact. duckdb::ErrorData's string constructor keeps non-JSON input
		// verbatim, which is what makes the net safe to apply to every exception an endpoint can see.
		REQUIRE(duckdb::ErrorData(ex).RawMessage() == std::string(ex.what()));
		REQUIRE_THAT(std::string(ex.what()), Catch::Matchers::ContainsSubstring("Could not query table: "));
		REQUIRE_THAT(std::string(ex.what()), Catch::Matchers::ContainsSubstring("Catalog Error:"));
	}
	REQUIRE(threw);
}

TEST_CASE("the safety net also turns a std::bad_alloc into a recoverable out-of-memory error", "[sql_generator]") {
	bool threw = false;
	try {
		throw std::bad_alloc();
	} catch (const std::exception& ex) {
		threw = true;
		// duckdb::ErrorData maps std::bad_alloc's what() to ExceptionType::OUT_OF_MEMORY.
		REQUIRE(recoverable_oom_message(duckdb::ErrorData(ex)).has_value());
	}
	REQUIRE(threw);
}
