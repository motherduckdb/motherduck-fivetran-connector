#pragma once

#include "duckdb.hpp"

#include <cstdint>
#include <optional>
#include <string>

// DuckDB default is DECIMAL(18,3)
constexpr std::uint8_t DECIMAL_DEFAULT_WIDTH = 18;
constexpr std::uint8_t DECIMAL_DEFAULT_SCALE = 3;
constexpr std::uint8_t DECIMAL_MIN_WIDTH = 1;
constexpr std::uint8_t DECIMAL_MAX_WIDTH = 38;

struct column_def {
	std::string name;
	duckdb::LogicalTypeId type = duckdb::LogicalTypeId::INVALID;
	std::optional<std::string> column_default;
	bool primary_key = false;
	// width and scale are only applicable for DECIMAL types.
	// The width is a number from 1 to 38 that indicates the total number of digits that can be stored.
	std::optional<std::uint8_t> width;
	// Scale is a number from 0 to the width that indicates the number of digits that can be stored after the decimal
	// point. In other words, it can be zero.
	std::optional<std::uint8_t> scale;
};

inline std::string format_type(const column_def& col) {
	if (col.type == duckdb::LogicalTypeId::DECIMAL && col.width.has_value()) {
		assert(col.width.value() >= DECIMAL_MIN_WIDTH && col.width.value() <= DECIMAL_MAX_WIDTH);
		assert(!col.scale.has_value() || col.scale.value() <= col.width.value());
		// If scale is not set, its value is 0. This is the same as for e.g. a DECIMAL(15) type in DuckDB.
		return duckdb::EnumUtil::ToString(col.type) + " (" + std::to_string(col.width.value()) + "," +
		       std::to_string(col.scale.value_or(0)) + ")";
	}
	return duckdb::EnumUtil::ToString(col.type);
}

struct table_def {
	std::string db_name;
	std::string schema_name;
	std::string table_name;

	[[nodiscard]] std::string to_escaped_string() const;
};

inline bool is_fivetran_system_column(const std::string& column_name) {
	return column_name == "_fivetran_start" || column_name == "_fivetran_end" || column_name == "_fivetran_active" ||
	       column_name == "_fivetran_deleted" || column_name == "_fivetran_synced";
}
