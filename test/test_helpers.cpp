#include "catch2/matchers/catch_matchers_string.hpp"
#include "constants.hpp"
#include "csv_processor.hpp"
#include "duckdb.hpp"
#include "integration/common.hpp"
#include "md_error.hpp"
#include "schema_types.hpp"
#include "sql_generator.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <filesystem>
#include <fstream>
#include <string>
#include <tuple>
#include <vector>

namespace fs = std::filesystem;
using namespace test::constants;

TEST_CASE("Test join strings", "[helpers]") {
	{
		const std::vector test_vec {1, 2, 3};
		REQUIRE(join(test_vec) == "1, 2, 3");
		REQUIRE(join(test_vec, " AND ") == "1 AND 2 AND 3");
	}

	{
		const std::vector<const column_def> test_vec {
		    column_def {.name = "id", .type = duckdb::LogicalTypeId::INTEGER, .primary_key = true},
		    column_def {.name = "title", .type = duckdb::LogicalTypeId::VARCHAR},
		    column_def {.name = "magic_number", .type = duckdb::LogicalTypeId::INTEGER},
		};

		auto joined = join(test_vec, [](std::ostream& out, const column_def& col) { out << col.quoted(); });
		REQUIRE(joined == "\"id\", \"title\", \"magic_number\"");

		std::ostringstream os;
		join(os, test_vec, "-del-", [](std::ostream& out, const column_def& col) { out << col.quoted(); });
		REQUIRE(os.str() == "\"id\"-del-\"title\"-del-\"magic_number\"");
	}
}
