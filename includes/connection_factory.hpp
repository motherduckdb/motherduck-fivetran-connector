#pragma once

#include "duckdb.hpp"
#include "md_logging.hpp"

#include <mutex>
#include <string>

class ConnectionFactory {
public:
    explicit ConnectionFactory() : stdout_logger(mdlog::Logger::CreateStdoutLogger()) {}
    duckdb::Connection GetConnection(const std::string &md_auth_token, const std::string &db_name);

private:
    duckdb::DuckDB &get_duckdb(const std::string &md_auth_token, const std::string &db_name);

    mdlog::Logger stdout_logger;
    std::once_flag db_init_flag;
    duckdb::DuckDB db;
    std::string initial_md_token;
};
