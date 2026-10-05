#include "../constants.hpp"
#include "common.hpp"
#include "md_logging.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <string>

using namespace test::constants;

namespace {
/// Sets MD_DISABLE_DUCKDB_LOGGING until the object goes out of scope again
class ScopedDisableFlag {
public:
	explicit ScopedDisableFlag(const char* value) {
		const char* previous = std::getenv(ENV_VAR);
		had_previous = previous != nullptr;
		if (had_previous) {
			previous_value = previous;
		}
		set(value);
	}

	~ScopedDisableFlag() {
		if (had_previous) {
			set(previous_value.c_str());
		} else {
			unsetenv(ENV_VAR);
		}
	}

	ScopedDisableFlag(const ScopedDisableFlag&) = delete;
	ScopedDisableFlag& operator=(const ScopedDisableFlag&) = delete;

private:
	static constexpr const char* ENV_VAR = "MD_DISABLE_DUCKDB_LOGGING";

	static void set(const char* value) {
		setenv(ENV_VAR, value, 1);
	}

	bool had_previous;
	std::string previous_value;
};
} // namespace

TEST_CASE("duckdb_logging_enabled follows MD_DISABLE_DUCKDB_LOGGING", "[logging]") {
	{
		const ScopedDisableFlag flag("1");
		REQUIRE_FALSE(mdlog::duckdb_logging_enabled());
	}
	{
		const ScopedDisableFlag flag("anything");
		REQUIRE_FALSE(mdlog::duckdb_logging_enabled());
	}
	{
		const ScopedDisableFlag flag("0");
		REQUIRE(mdlog::duckdb_logging_enabled());
	}
}

// Without this call the destination produces no DuckDB-side logs at all, so pin
// down the exact query against the DuckDB and MotherDuck versions in use.
TEST_CASE("initialize_duckdb_logging enables logging on the instance", "[integration][logging]") {
	const auto con = get_test_connection(MD_TOKEN);

	REQUIRE_FALSE(mdlog::initialize_duckdb_logging(*con).has_value());

	const auto enabled_res = con->Query("SELECT current_setting('enable_logging')");
	REQUIRE_NO_FAIL(enabled_res);
	REQUIRE(enabled_res->GetValue(0, 0).GetValue<bool>());

	const auto storage_res = con->Query("SELECT current_setting('logging_storage')");
	REQUIRE_NO_FAIL(storage_res);
	REQUIRE(storage_res->GetValue(0, 0).ToString() == "motherduck_log_storage");

	// write_log is what mdlog::Logger uses for its DuckDB sink.
	REQUIRE_NO_FAIL(con->Query("SELECT write_log('test message', log_type:='Fivetran', level:='INFO')"));
}
