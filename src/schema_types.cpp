#include "schema_types.hpp"
#include "duckdb.hpp"

#include <sstream>
#include <string>

std::string table_def::to_escaped_string() const {
    std::ostringstream out;
    out << duckdb::KeywordHelper::WriteQuoted(db_name, '"') << "."
        << duckdb::KeywordHelper::WriteQuoted(schema_name, '"') << "."
        << duckdb::KeywordHelper::WriteQuoted(table_name, '"');
    return out.str();
}