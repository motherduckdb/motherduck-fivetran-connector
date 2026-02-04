#pragma once

#include "connection_factory.hpp"
#include "destination_sdk.grpc.pb.h"

class DestinationSdkImpl final : public fivetran_sdk::v2::DestinationConnector::Service {
public:
	explicit DestinationSdkImpl() = default;
	~DestinationSdkImpl() override = default;

	::grpc::Status ConfigurationForm(::grpc::ServerContext *context,
	                                 const ::fivetran_sdk::v2::ConfigurationFormRequest *request,
	                                 ::fivetran_sdk::v2::ConfigurationFormResponse *response) override;
	::grpc::Status Test(::grpc::ServerContext *context, const ::fivetran_sdk::v2::TestRequest *request,
	                    ::fivetran_sdk::v2::TestResponse *response) override;

	::grpc::Status Capabilities(::grpc::ServerContext *context, const ::fivetran_sdk::v2::CapabilitiesRequest *request,
	                            ::fivetran_sdk::v2::CapabilitiesResponse *response) override;
	::grpc::Status DescribeTable(::grpc::ServerContext *context,
	                             const ::fivetran_sdk::v2::DescribeTableRequest *request,
	                             ::fivetran_sdk::v2::DescribeTableResponse *response) override;
	::grpc::Status CreateTable(::grpc::ServerContext *context, const ::fivetran_sdk::v2::CreateTableRequest *request,
	                           ::fivetran_sdk::v2::CreateTableResponse *response) override;
	::grpc::Status AlterTable(::grpc::ServerContext *context, const ::fivetran_sdk::v2::AlterTableRequest *request,
	                          ::fivetran_sdk::v2::AlterTableResponse *response) override;
	::grpc::Status Truncate(::grpc::ServerContext *context, const ::fivetran_sdk::v2::TruncateRequest *request,
	                        ::fivetran_sdk::v2::TruncateResponse *response) override;
	::grpc::Status WriteBatch(::grpc::ServerContext *context, const ::fivetran_sdk::v2::WriteBatchRequest *request,
	                          ::fivetran_sdk::v2::WriteBatchResponse *response) override;
	::grpc::Status WriteHistoryBatch(::grpc::ServerContext *context,
	                                 const ::fivetran_sdk::v2::WriteHistoryBatchRequest *request,
	                                 ::fivetran_sdk::v2::WriteBatchResponse *response) override;
	::grpc::Status Migrate(::grpc::ServerContext *context, const ::fivetran_sdk::v2::MigrateRequest *request,
	                       ::fivetran_sdk::v2::MigrateResponse *response) override;

private:
	ConnectionFactory connection_factory;
};
