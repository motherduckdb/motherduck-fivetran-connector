#pragma once

#include "destination_sdk.grpc.pb.h"
#include "duckdb.hpp"
#include "md_logging.hpp"

#include <memory>
#include <mutex>

class DestinationSdkImpl final
    : public fivetran_sdk::v2::DestinationConnector::Service {
public:
  DestinationSdkImpl() = default;
  ~DestinationSdkImpl() override = default;

  ::grpc::Status ConfigurationForm(
      ::grpc::ServerContext *context,
      const ::fivetran_sdk::v2::ConfigurationFormRequest *request,
      ::fivetran_sdk::v2::ConfigurationFormResponse *response) override;
  ::grpc::Status Test(::grpc::ServerContext *context,
                      const ::fivetran_sdk::v2::TestRequest *request,
                      ::fivetran_sdk::v2::TestResponse *response) override;

  ::grpc::Status
  Capabilities(::grpc::ServerContext *context,
               const ::fivetran_sdk::v2::CapabilitiesRequest *request,
               ::fivetran_sdk::v2::CapabilitiesResponse *response) override;
  ::grpc::Status
  DescribeTable(::grpc::ServerContext *context,
                const ::fivetran_sdk::v2::DescribeTableRequest *request,
                ::fivetran_sdk::v2::DescribeTableResponse *response) override;
  ::grpc::Status
  CreateTable(::grpc::ServerContext *context,
              const ::fivetran_sdk::v2::CreateTableRequest *request,
              ::fivetran_sdk::v2::CreateTableResponse *response) override;
  ::grpc::Status
  AlterTable(::grpc::ServerContext *context,
             const ::fivetran_sdk::v2::AlterTableRequest *request,
             ::fivetran_sdk::v2::AlterTableResponse *response) override;
  ::grpc::Status
  Truncate(::grpc::ServerContext *context,
           const ::fivetran_sdk::v2::TruncateRequest *request,
           ::fivetran_sdk::v2::TruncateResponse *response) override;
  ::grpc::Status
  WriteBatch(::grpc::ServerContext *context,
             const ::fivetran_sdk::v2::WriteBatchRequest *request,
             ::fivetran_sdk::v2::WriteBatchResponse *response) override;
  ::grpc::Status
  WriteHistoryBatch(::grpc::ServerContext *context,
                    const ::fivetran_sdk::v2::WriteHistoryBatchRequest *request,
                    ::fivetran_sdk::v2::WriteBatchResponse *response) override;

private:
  std::string initial_md_token;
  duckdb::DuckDB db;
  std::once_flag db_init_flag;

  duckdb::DuckDB &get_duckdb(const std::string &md_auth_token,
                             const std::string &db_name,
                             const std::shared_ptr<mdlog::MdLog> &logger);
  std::unique_ptr<duckdb::Connection> get_connection(
      const std::string &md_auth_token,
      const std::string &db_name, const std::shared_ptr<mdlog::MdLog> &logger);
};
