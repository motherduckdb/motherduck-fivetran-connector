#include "duckdb.hpp"
#include "integration/common.hpp"
#include "md_error.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <exception>
#include <new>
#include <optional>
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
	REQUIRE_THAT(*message, Catch::Matchers::ContainsSubstring("larger MotherDuck instance size"));
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
	                    Catch::Matchers::ContainsSubstring("larger MotherDuck instance size"));
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

// The tests below cover what the safety net in motherduck_destination_server.cpp is built from:
// recoverable_oom_message() over a duckdb::ErrorData reconstructed from a caught exception's what().

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
		REQUIRE_THAT(*message, Catch::Matchers::ContainsSubstring("larger MotherDuck instance size"));
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
		// ...with the readable message intact: ErrorData keeps a message not starting with '{' verbatim.
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
		const auto message = recoverable_oom_message(duckdb::ErrorData(ex));
		REQUIRE(message.has_value());
		// A bad_alloc is the connector process running out, not the destination, so the instance-size
		// advice does not fit this case. Accepted deliberately: the destination is the common case, the
		// wording is hedged, and the embedded original error carries no duckling suffix when it was local.
		REQUIRE_THAT(*message, Catch::Matchers::ContainsSubstring("larger MotherDuck instance size"));
	}
	REQUIRE(threw);
}

TEST_CASE("reconstructing an ErrorData from a malformed JSON message throws", "[sql_generator]") {
	// Pins the hazard the net's try/catch exists for. NOTE: this does not exercise that try/catch, which
	// lives in an anonymous namespace in motherduck_destination_server.cpp and is not reachable from here --
	// delete it and this test still passes.
	REQUIRE_THROWS(duckdb::ErrorData(std::string("{not valid json")));

	// The shape the net relies on, reproduced here rather than called: catching yields no verdict.
	std::optional<std::string> verdict;
	REQUIRE_NOTHROW([&] {
		try {
			verdict = recoverable_oom_message(duckdb::ErrorData(std::string("{not valid json")));
		} catch (const std::exception&) {
			verdict = std::nullopt;
		}
	}());
	REQUIRE_FALSE(verdict.has_value());
}
