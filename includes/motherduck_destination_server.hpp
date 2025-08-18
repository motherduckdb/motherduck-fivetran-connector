#pragma once

#include "destination_sdk.grpc.pb.h"
#include "duckdb.hpp"
#include <md_logging.hpp>

static constexpr const char *const MD_PROP_DATABASE = "motherduck_database";

static constexpr const char *const MD_PROP_TOKEN = "motherduck_token";

static constexpr const char *const MD_PROP_CSV_BLOCK_SIZE =
    "motherduck_csv_block_size";

static constexpr const char *const CONFIG_TEST_NAME_AUTHENTICATE =
    "test_authentication";

static constexpr const char *const CONFIG_TEST_NAME_CSV_BLOCK_SIZE =
    "test_csv_block_size";

static constexpr const int DUCKDB_DEFAULT_PRECISION = 18;

static constexpr const int DUCKDB_DEFAULT_SCALE = 3;

class DestinationSdkImpl final
    : public fivetran_sdk::v2::DestinationConnector::Service {
public:
  explicit DestinationSdkImpl();
  ~DestinationSdkImpl() = default;

  /** Creates a duckdb instance, loads extensions and initializes the passed-in
   * logger with the user token */
  std::unique_ptr<duckdb::Connection> InitConnection(
      const google::protobuf::Map<std::string, std::string> &request_config,
      const std::string &db_name, const std::shared_ptr<mdlog::MdLog> &logger);

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
  std::shared_ptr<logging_sink::LoggingSink::Stub> loggingSinkClient = nullptr;
};
