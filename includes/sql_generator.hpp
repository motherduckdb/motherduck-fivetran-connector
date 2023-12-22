#include "duckdb.hpp"


struct column_def {
    std::string name;
    duckdb::LogicalTypeId type;
    bool primary_key;
    unsigned int width;
    unsigned int scale;
};

bool schema_exists(duckdb::Connection &con, const std::string db_name, const std::string schema_name);

void create_schema(duckdb::Connection &con, std::string db_name, std::string schema_name);

bool table_exists(duckdb::Connection &con, const std::string db_name, const std::string schema_name,
                  const std::string table_name);

void create_table(duckdb::Connection &con, std::string db_name, std::string schema_name, std::string table_name,
                  std::vector<column_def> columns);

std::vector<column_def>
describe_table(duckdb::Connection &con, std::string db_name, std::string schema_name, std::string table_name);

void alter_table(duckdb::Connection &con, std::string db_name, std::string schema_name, std::string table_name,
                 std::vector<column_def> columns);

void upsert(duckdb::Connection &con, std::string db_name, std::string schema_name, std::string table_name,
            std::string staging_table_name, const std::vector<std::string> primary_keys,
            std::vector<column_def> columns);

void update_values(duckdb::Connection &con, std::string db_name, std::string schema_name, std::string table_name,
                   std::string staging_table_name, const std::vector<std::string> primary_keys,
                   std::vector<column_def> columns);

void truncate_table(duckdb::Connection &con, std::string db_name, std::string schema_name, std::string table_name);

void delete_rows(duckdb::Connection &con, std::string db_name, std::string schema_name, std::string table_name,
                 std::string staging_table_name, const std::vector<std::string> primary_keys);

void check_connection(duckdb::Connection &con);