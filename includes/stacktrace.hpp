// Copied from DuckDB.
// Original at https://github.com/duckdb/duckdb/blob/v1.3.2/src/include/duckdb/common/stacktrace.hpp.

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/stacktrace.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <cstdint>

namespace duckdb_copy {

    class StackTrace {
    public:
        static std::string GetStacktracePointers(uint64_t max_depth = 120);
        static std::string ResolveStacktraceSymbols(const std::string &pointers);

        inline static std::string GetStackTrace(uint64_t max_depth = 120) {
            return ResolveStacktraceSymbols(GetStacktracePointers(max_depth));
        }
    };

} // namespace duckdb_copy
