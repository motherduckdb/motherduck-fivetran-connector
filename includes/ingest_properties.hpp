#pragma once

#include "schema_types.hpp"

#include <cstdint>
#include <string>
#include <vector>

inline constexpr std::uint32_t MAX_PARALLEL_REQUESTS = 8;

/// We have to at some point handle up to eight parallel WriteBatch requests
/// that all allocate a buffer of buffer_size. The container memory limit is 1
/// (or 2?) GiB. Assuming the worst case that all eight requests arrive at the
/// same time, we need to limit the buffer size accordingly. We don't want to
/// come too close to the limit, so we pick 768 MiB here. Originally, this was
/// set to 512 MiB, but one user actually had a line size of over 20 MiB.
inline constexpr std::uint32_t MAX_LINE_SIZE_DEFAULT = 768 / MAX_PARALLEL_REQUESTS / 4; // 24 MiB

struct IngestProperties {
	const std::string filename;
	/// Binary key used to decrypt the CSV file. Empty if the file is not
	/// encrypted.
	const std::string decryption_key;
	/// Columns of the table that is being ingested into. Columns must be in the
	/// same order as they appear in the table.
	const std::vector<column_def> columns;
	/// String that represents NULL values in the CSV file.
	const std::string null_value;
	/// Indicates that the CSV file may contain "unmodified_string" values that
	/// should be treated as strings even if the target column is of a different
	/// type. In that case, the CSV file is read with all_varchar=true and type
	/// conversion is deferred to later stages (i.e., UPDATE).
	const bool allow_unmodified_string = false;
	/// Optional user-configured max_line_size (in MiB) for DuckDB's read_csv.
	const std::uint32_t max_line_size = MAX_LINE_SIZE_DEFAULT;
};
