#pragma once

#include "duckdb.hpp"

#include <cstdint>
#include <optional>
#include <string>

struct column_def {
	std::string name;
	duckdb::LogicalTypeId type;
	std::optional<std::string> column_default;
	bool primary_key;
	std::uint8_t width;
	std::uint8_t scale;
};

inline std::string format_type(const column_def& col) {
	if (col.type == duckdb::LogicalTypeId::DECIMAL && col.width > 0 and col.scale > 0) {
		return duckdb::EnumUtil::ToString(col.type) + " (" + std::to_string(col.width) + "," +
		       std::to_string(col.scale) + ")";
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
	if (column_name == "_fivetran_start" || column_name == "_fivetran_end" || column_name == "_fivetran_active" ||
	    column_name == "_fivetran_deleted" || column_name == "_fivetran_synced") {
		return true;
	}
	return false;
}
