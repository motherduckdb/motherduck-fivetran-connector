#pragma once

#include "destination_sdk.grpc.pb.h"

static constexpr const char *const MD_PROP_DATABASE = "motherduck_database";

static constexpr const char *const MD_PROP_TOKEN = "motherduck_token";

class DestinationSdkImpl final : public fivetran_sdk::Destination::Service {
public:
  DestinationSdkImpl() = default;
  ~DestinationSdkImpl() = default;
  ::grpc::Status ConfigurationForm(
      ::grpc::ServerContext *context,
      const ::fivetran_sdk::ConfigurationFormRequest *request,
      ::fivetran_sdk::ConfigurationFormResponse *response) override;
  ::grpc::Status Test(::grpc::ServerContext *context,
                      const ::fivetran_sdk::TestRequest *request,
                      ::fivetran_sdk::TestResponse *response) override;
  ::grpc::Status
  DescribeTable(::grpc::ServerContext *context,
                const ::fivetran_sdk::DescribeTableRequest *request,
                ::fivetran_sdk::DescribeTableResponse *response) override;
  ::grpc::Status
  CreateTable(::grpc::ServerContext *context,
              const ::fivetran_sdk::CreateTableRequest *request,
              ::fivetran_sdk::CreateTableResponse *response) override;
  ::grpc::Status
  AlterTable(::grpc::ServerContext *context,
             const ::fivetran_sdk::AlterTableRequest *request,
             ::fivetran_sdk::AlterTableResponse *response) override;
  ::grpc::Status Truncate(::grpc::ServerContext *context,
                          const ::fivetran_sdk::TruncateRequest *request,
                          ::fivetran_sdk::TruncateResponse *response) override;
  ::grpc::Status
  WriteBatch(::grpc::ServerContext *context,
             const ::fivetran_sdk::WriteBatchRequest *request,
             ::fivetran_sdk::WriteBatchResponse *response) override;
};
