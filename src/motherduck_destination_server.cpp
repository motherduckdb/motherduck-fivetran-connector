#include "motherduck_destination_server.hpp"

#include "config.hpp"
#include "config_tester.hpp"
#include "csv_processor.hpp"
#include "decryption.hpp"
#include "destination_sdk.grpc.pb.h"
#include "duckdb.hpp"
#include "fivetran_duckdb_interop.hpp"
#include "ingest_properties.hpp"
#include "md_error.hpp"
#include "md_logging.hpp"
#include "request_context.hpp"
#include "sql_generator.hpp"

#include <exception>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <optional>
#include <string>

namespace {
::grpc::Status create_grpc_status_from_exception(const std::exception& ex, const std::string& prefix = "") {
	const std::string error_message = md_error::truncate_for_grpc_header(ex.what());
	// The assumption here is that the prefix is short enough that its length can
	// be disregarded
	return ::grpc::Status(::grpc::StatusCode::INTERNAL, prefix + error_message);
}

template <typename T>
std::string get_schema_name(const T* request) {
	std::string schema_name = request->schema_name();
	if (schema_name.empty()) {
		return "main";
	}
	return schema_name;
}

template <typename T>
std::string get_table_name(const T* request) {
	std::string table_name = request->table_name();
	if (table_name.empty()) {
		throw std::invalid_argument("Table name cannot be empty");
	}
	return table_name;
}

std::vector<column_def>
get_duckdb_columns(const google::protobuf::RepeatedPtrField<fivetran_sdk::v2::Column>& fivetran_columns) {
	std::vector<column_def> duckdb_columns;
	for (auto& col : fivetran_columns) {
		const duckdb::LogicalTypeId duckdb_type = get_duckdb_type(col.type());
		if (duckdb_type == duckdb::LogicalTypeId::INVALID) {
			throw std::invalid_argument("Cannot convert Fivetran type <" + DataType_Name(col.type()) +
			                            "> for column <" + col.name() + "> to a DuckDB type");
		}

		std::uint8_t decimal_width = 0;
		std::uint8_t decimal_scale = 0;
		if (duckdb_type == duckdb::LogicalTypeId::DECIMAL) {
			if (col.has_params() && col.params().has_decimal()) {
				const std::uint32_t fivetran_precision = col.params().decimal().precision();
				const std::uint32_t fivetran_scale = col.params().decimal().scale();

				// Maximum width supported by DuckDB is 38
				if (fivetran_precision > 38) {
					throw std::invalid_argument("Decimal width " + std::to_string(fivetran_precision) +
					                            " for column <" + col.name() +
					                            "> exceeds maximum supported width of 38 in DuckDB");
				}

				if (fivetran_scale > fivetran_precision) {
					throw std::invalid_argument("Decimal scale " + std::to_string(fivetran_scale) + " for column <" +
					                            col.name() + "> cannot be greater than precision " +
					                            std::to_string(fivetran_precision));
				}

				decimal_width = static_cast<std::uint8_t>(fivetran_precision);
				decimal_scale = static_cast<std::uint8_t>(fivetran_scale);
			} else {
				// DuckDB default is DECIMAL(18, 3)
				decimal_width = 18;
				decimal_scale = 3;
			}
		}

		duckdb_columns.push_back(
		    column_def {col.name(), duckdb_type, std::nullopt, col.primary_key(), decimal_width, decimal_scale});
	}
	return duckdb_columns;
}

std::string get_decryption_key(const std::string& filename, const google::protobuf::Map<std::string, std::string>& keys,
                               const ::fivetran_sdk::v2::Encryption encryption) {
	if (encryption == ::fivetran_sdk::v2::Encryption::NONE) {
		return "";
	}

	const auto encryption_key_it = keys.find(filename);
	if (encryption_key_it == keys.end()) {
		throw std::invalid_argument("Missing encryption key for " + filename);
	}

	return encryption_key_it->second;
}

std::uint32_t get_max_record_size(const google::protobuf::Map<std::string, std::string>& configuration,
                                  mdlog::Logger& logger) {
	const auto value = config::find_optional_property(configuration, config::PROP_MAX_RECORD_SIZE);

	std::uint32_t max_record_size = MAX_RECORD_SIZE_DEFAULT;
	if (value.has_value()) {
		unsigned long converted_value;
		try {
			converted_value = std::stoul(value.value());
		} catch (const std::exception&) {
			throw md_error::RecoverableError("Value \"" + value.value() +
			                                 "\" could not be converted into an integer for \"Max Record Size\". "
			                                 "Make sure to set the \"Max Record Size\" to a valid positive integer.");
		}

		// Only use max_record_size values from the configuration that are larger than the default
		if (converted_value >= MAX_RECORD_SIZE_DEFAULT && converted_value <= MAX_RECORD_SIZE_MAX) {
			max_record_size = static_cast<std::uint32_t>(converted_value);
		} else if (converted_value < MAX_RECORD_SIZE_DEFAULT) {
			logger.warning("Value \"" + value.value() +
			               "\" of \"Max Record Size\" is too low, "
			               "using default of 24 MiB.");
		} else { // Value must be too high
			logger.warning("Value \"" + value.value() +
			               "\" of \"Max Record Size\" is too high, "
			               "using maximum of 1024 MiB.");
		}
	}

	return max_record_size;
}
} // namespace

grpc::Status DestinationSdkImpl::ConfigurationForm(::grpc::ServerContext*,
                                                   const ::fivetran_sdk::v2::ConfigurationFormRequest*,
                                                   ::fivetran_sdk::v2::ConfigurationFormResponse* response) {

	response->set_schema_selection_supported(true);
	response->set_table_selection_supported(true);

	fivetran_sdk::v2::FormField token_field;
	token_field.set_name(config::PROP_TOKEN);
	token_field.set_label("Authentication Token");
	token_field.set_description("Please get your authentication token from app.motherduck.com");
	token_field.set_text_field(fivetran_sdk::v2::Password);
	token_field.set_required(true);
	response->add_fields()->CopyFrom(token_field);

	fivetran_sdk::v2::FormField db_field;
	db_field.set_name(config::PROP_DATABASE);
	db_field.set_label("Database Name");
	db_field.set_description("The database to work in. The database must already "
	                         "exist and be writable.");
	db_field.set_text_field(fivetran_sdk::v2::PlainText);
	db_field.set_required(true);
	response->add_fields()->CopyFrom(db_field);

	fivetran_sdk::v2::FormField max_record_size_field;
	max_record_size_field.set_name(config::PROP_MAX_RECORD_SIZE);
	max_record_size_field.set_label("Max Record Size (MiB)");
	max_record_size_field.set_description(
	    "This should be a positive integer between 24 and 1048, without any units. Other units provided will be"
	    " ignored. Internally, this is an upper limit for the lines in the CSV files"
	    " Fivetran generates. Increase this if the ingest fails and the error suggests to increase the"
	    " \"Max Record Size (MiB)\" option, or if you are certain you have very large records. Leave empty to use the"
	    " default (24 MiB). Warning: setting this too high can lead to out-of-memory errors for high-volume ingests.");
	max_record_size_field.set_text_field(fivetran_sdk::v2::PlainText);
	max_record_size_field.set_required(false);
	response->add_fields()->CopyFrom(max_record_size_field);

	for (const auto& test_case : config_tester::get_test_cases()) {
		auto connection_test = response->add_tests();
		connection_test->set_name(test_case.name);
		connection_test->set_label(test_case.description);
	}

	return ::grpc::Status::OK;
}

grpc::Status DestinationSdkImpl::Capabilities(::grpc::ServerContext*, const ::fivetran_sdk::v2::CapabilitiesRequest*,
                                              ::fivetran_sdk::v2::CapabilitiesResponse* response) {
	response->set_batch_file_format(::fivetran_sdk::v2::CSV);
	return ::grpc::Status::OK;
}

grpc::Status DestinationSdkImpl::DescribeTable(::grpc::ServerContext*,
                                               const ::fivetran_sdk::v2::DescribeTableRequest* request,
                                               ::fivetran_sdk::v2::DescribeTableResponse* response) {
	std::optional<RequestContext> ctx;
	try {
		ctx.emplace("DescribeTable", connection_factory, request->configuration());
	} catch (const std::exception& ex) {
		return create_grpc_status_from_exception(ex);
	}
	auto& con = ctx->GetConnection();
	auto& logger = ctx->GetLogger();

	try {
		std::string db_name = config::find_property(request->configuration(), config::PROP_DATABASE);
		auto sql_generator = std::make_unique<MdSqlGenerator>(logger);
		table_def table_name {db_name, get_schema_name(request), get_table_name(request)};
		logger.info("Endpoint <DescribeTable>: schema name <" + table_name.schema_name + ">");
		logger.info("Endpoint <DescribeTable>: table name <" + table_name.table_name + ">");
		if (!sql_generator->table_exists(con, table_name)) {
			logger.info("Endpoint <DescribeTable>: table not found");
			response->set_not_found(true);
			return ::grpc::Status::OK;
		}

		logger.info("Endpoint <DescribeTable>: table exists; getting columns");
		auto duckdb_columns = sql_generator->describe_table(con, table_name);
		logger.info("Endpoint <DescribeTable>: got " + std::to_string(duckdb_columns.size()) + " columns");

		fivetran_sdk::v2::Table* table = response->mutable_table();
		table->set_name(get_table_name(request));

		for (auto& col : duckdb_columns) {
			fivetran_sdk::v2::Column* ft_col = table->mutable_columns()->Add();
			ft_col->set_name(col.name);
			const auto fivetran_type = get_fivetran_type(col.type);
			ft_col->set_type(fivetran_type);
			ft_col->set_primary_key(col.primary_key);
			if (fivetran_type == fivetran_sdk::v2::DECIMAL) {
				ft_col->mutable_params()->mutable_decimal()->set_precision(col.width);
				ft_col->mutable_params()->mutable_decimal()->set_scale(col.scale);
			}
		}

	} catch (const md_error::RecoverableError& mde) {
		logger.warning("DescribeTable endpoint failed for schema <" + request->schema_name() + ">, table <" +
		               request->table_name() + ">:" + std::string(mde.what()));
		response->mutable_task()->set_message(mde.what());
		return ::grpc::Status::OK;
	} catch (const std::exception& ex) {
		logger.severe("DescribeTable endpoint failed for schema <" + request->schema_name() + ">, table <" +
		              request->table_name() + ">:" + std::string(ex.what()));
		response->mutable_task()->set_message(ex.what());
		return create_grpc_status_from_exception(ex);
	}

	return ::grpc::Status::OK;
}

grpc::Status DestinationSdkImpl::CreateTable(::grpc::ServerContext*,
                                             const ::fivetran_sdk::v2::CreateTableRequest* request,
                                             ::fivetran_sdk::v2::CreateTableResponse* response) {
	std::optional<RequestContext> ctx;
	try {
		ctx.emplace("CreateTable", connection_factory, request->configuration());
	} catch (const std::exception& ex) {
		return create_grpc_status_from_exception(ex);
	}
	auto& con = ctx->GetConnection();
	auto& logger = ctx->GetLogger();

	try {
		auto schema_name = get_schema_name(request);

		std::string db_name = config::find_property(request->configuration(), config::PROP_DATABASE);
		auto sql_generator = std::make_unique<MdSqlGenerator>(logger);
		const table_def table {db_name, schema_name, request->table().name()};

		sql_generator->create_schema_if_not_exists(con, db_name, schema_name);
		const auto cols = get_duckdb_columns(request->table().columns());
		sql_generator->create_table(con, table, cols, {});
		response->set_success(true);
	} catch (const md_error::RecoverableError& mde) {
		logger.warning("CreateTable endpoint failed for schema <" + request->schema_name() + ">, table <" +
		               request->table().name() + ">:" + std::string(mde.what()));
		response->mutable_task()->set_message(mde.what());
		return ::grpc::Status::OK;
	} catch (const std::exception& ex) {
		logger.severe("CreateTable endpoint failed for schema <" + request->schema_name() + ">, table <" +
		              request->table().name() + ">:" + std::string(ex.what()));
		response->mutable_task()->set_message(ex.what());
		return create_grpc_status_from_exception(ex);
	}

	return ::grpc::Status::OK;
}

grpc::Status DestinationSdkImpl::AlterTable(::grpc::ServerContext*,
                                            const ::fivetran_sdk::v2::AlterTableRequest* request,
                                            ::fivetran_sdk::v2::AlterTableResponse* response) {
	std::optional<RequestContext> ctx;
	try {
		ctx.emplace("AlterTable", connection_factory, request->configuration());
	} catch (const std::exception& ex) {
		return create_grpc_status_from_exception(ex);
	}
	auto& con = ctx->GetConnection();
	auto& logger = ctx->GetLogger();

	try {
		std::string db_name = config::find_property(request->configuration(), config::PROP_DATABASE);
		table_def table_name {db_name, get_schema_name(request), request->table().name()};

		auto sql_generator = std::make_unique<MdSqlGenerator>(logger);
		sql_generator->alter_table(con, table_name, get_duckdb_columns(request->table().columns()),
		                           request->drop_columns());
		response->set_success(true);
	} catch (const md_error::RecoverableError& mde) {
		logger.severe("AlterTable endpoint failed for schema <" + request->schema_name() + ">, table <" +
		              request->table().name() + ">:" + std::string(mde.what()));
		response->mutable_task()->set_message(mde.what());
		return ::grpc::Status::OK;
	} catch (const std::exception& ex) {
		logger.severe("AlterTable endpoint failed for schema <" + request->schema_name() + ">, table <" +
		              request->table().name() + ">:" + std::string(ex.what()));
		response->mutable_task()->set_message(ex.what());
		return create_grpc_status_from_exception(ex);
	}

	return ::grpc::Status::OK;
}

grpc::Status DestinationSdkImpl::Truncate(::grpc::ServerContext*, const ::fivetran_sdk::v2::TruncateRequest* request,
                                          ::fivetran_sdk::v2::TruncateResponse* response) {
	std::optional<RequestContext> ctx;
	try {
		ctx.emplace("Truncate", connection_factory, request->configuration());
	} catch (const std::exception& ex) {
		return create_grpc_status_from_exception(ex);
	}
	auto& con = ctx->GetConnection();
	auto& logger = ctx->GetLogger();

	try {
		std::string db_name = config::find_property(request->configuration(), config::PROP_DATABASE);
		table_def table_name {db_name, get_schema_name(request), get_table_name(request)};
		if (request->synced_column().empty()) {
			throw std::invalid_argument("Synced column is required");
		}

		auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

		if (sql_generator->table_exists(con, table_name)) {
			std::chrono::nanoseconds delete_before_ts = std::chrono::seconds(request->utc_delete_before().seconds()) +
			                                            std::chrono::nanoseconds(request->utc_delete_before().nanos());
			const std::string deleted_column = request->has_soft() ? request->soft().deleted_column() : "";
			sql_generator->truncate_table(con, table_name, request->synced_column(), delete_before_ts, deleted_column);
		} else {
			logger.warning("Table <" + request->table_name() + "> not found in schema <" + request->schema_name() +
			               ">; not truncated");
		}

	} catch (const md_error::RecoverableError& mde) {
		logger.warning("Truncate endpoint failed for schema <" + request->schema_name() + ">, table <" +
		               request->table_name() + ">:" + std::string(mde.what()));
		response->mutable_task()->set_message(mde.what());
		return ::grpc::Status::OK;
	} catch (const std::exception& ex) {
		logger.severe("Truncate endpoint failed for schema <" + request->schema_name() + ">, table <" +
		              request->table_name() + ">:" + std::string(ex.what()));
		response->mutable_task()->set_message(ex.what());
		return create_grpc_status_from_exception(ex);
	}

	return ::grpc::Status::OK;
}

grpc::Status DestinationSdkImpl::WriteBatch(::grpc::ServerContext*,
                                            const ::fivetran_sdk::v2::WriteBatchRequest* request,
                                            ::fivetran_sdk::v2::WriteBatchResponse* response) {
	std::optional<RequestContext> ctx;
	try {
		ctx.emplace("WriteBatch", connection_factory, request->configuration());
	} catch (const std::exception& ex) {
		return create_grpc_status_from_exception(ex);
	}
	auto& con = ctx->GetConnection();
	auto& logger = ctx->GetLogger();

	try {
		auto schema_name = get_schema_name(request);

		const std::string db_name = config::find_property(request->configuration(), config::PROP_DATABASE);
		const auto max_record_size = get_max_record_size(request->configuration(), logger);

		table_def table_name {db_name, get_schema_name(request), request->table().name()};
		auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

		const auto cols = get_duckdb_columns(request->table().columns());
		std::vector<const column_def*> columns_pk;
		std::vector<const column_def*> columns_regular;
		find_primary_keys(cols, columns_pk, &columns_regular);

		if (columns_pk.empty()) {
			throw std::invalid_argument("No primary keys found");
		}

		for (auto& filename : request->replace_files()) {
			logger.info("Processing replace file " + filename);
			const auto decryption_key =
			    get_decryption_key(filename, request->keys(), request->file_params().encryption());
			IngestProperties props {.filename = filename,
			                        .decryption_key = decryption_key,
			                        .columns = cols,
			                        .null_value = request->file_params().null_string(),
			                        .allow_unmodified_string = false,
			                        .max_record_size = max_record_size};

			csv_processor::ProcessFile(con, props, logger, [&](const std::string& staging_table_name) {
				sql_generator->upsert(con, table_name, staging_table_name, columns_pk, columns_regular);
			});
		}

		for (auto& filename : request->update_files()) {
			logger.info("Processing update file " + filename);
			auto decryption_key = get_decryption_key(filename, request->keys(), request->file_params().encryption());
			IngestProperties props {.filename = filename,
			                        .decryption_key = decryption_key,
			                        .columns = cols,
			                        .null_value = request->file_params().null_string(),
			                        .allow_unmodified_string = true,
			                        .max_record_size = max_record_size};

			csv_processor::ProcessFile(con, props, logger, [&](const std::string& staging_table_name) {
				sql_generator->update_values(con, table_name, staging_table_name, columns_pk, columns_regular,
				                             request->file_params().unmodified_string());
			});
		}

		for (auto& filename : request->delete_files()) {
			logger.info("Processing delete file " + filename);
			std::vector<column_def> cols_to_read;
			for (const auto& col : columns_pk) {
				cols_to_read.push_back(*col);
			}
			auto decryption_key = get_decryption_key(filename, request->keys(), request->file_params().encryption());
			IngestProperties props {.filename = filename,
			                        .decryption_key = decryption_key,
			                        .columns = cols_to_read,
			                        .null_value = request->file_params().null_string(),
			                        .allow_unmodified_string = false,
			                        .max_record_size = max_record_size};

			csv_processor::ProcessFile(con, props, logger, [&](const std::string& staging_table_name) {
				sql_generator->delete_rows(con, table_name, staging_table_name, columns_pk);
			});
		}

	} catch (const md_error::RecoverableError& mde) {
		auto const msg = "WriteBatch endpoint failed for schema <" + request->schema_name() + ">, table <" +
		                 request->table().name() + ">: " + std::string(mde.what());
		logger.warning(msg);
		response->mutable_task()->set_message(msg);
		return ::grpc::Status::OK;
	} catch (const std::exception& ex) {
		const std::string error_prefix = "WriteBatch endpoint failed for schema <" + request->schema_name() +
		                                 ">, table <" + request->table().name() + ">: ";
		const auto error_msg = error_prefix + ex.what();
		logger.severe(error_msg);
		response->mutable_task()->set_message(error_msg);
		return create_grpc_status_from_exception(ex, error_prefix);
	}

	return ::grpc::Status::OK;
}

::grpc::Status DestinationSdkImpl::WriteHistoryBatch(::grpc::ServerContext*,
                                                     const ::fivetran_sdk::v2::WriteHistoryBatchRequest* request,
                                                     ::fivetran_sdk::v2::WriteBatchResponse* response) {
	std::optional<RequestContext> ctx;
	try {
		ctx.emplace("WriteHistoryBatch", connection_factory, request->configuration());
	} catch (const std::exception& ex) {
		return create_grpc_status_from_exception(ex);
	}
	auto& con = ctx->GetConnection();
	auto& logger = ctx->GetLogger();
	auto sql_generator = std::make_unique<MdSqlGenerator>(logger);
	// We keep the table name in the outer scope to be able to drop the LAR table
	// in the catch block
	std::string lar_table_name;

	try {
		auto schema_name = get_schema_name(request);

		const std::string db_name = config::find_property(request->configuration(), config::PROP_DATABASE);
		const auto max_record_size = get_max_record_size(request->configuration(), logger);

		table_def table_name {db_name, get_schema_name(request), request->table().name()};

		const auto cols = get_duckdb_columns(request->table().columns());
		std::vector<const column_def*> columns_pk;
		std::vector<const column_def*> columns_regular;
		find_primary_keys(cols, columns_pk, &columns_regular, "_fivetran_start");
		if (columns_pk.empty()) {
			throw std::invalid_argument("No primary keys found");
		}

		/*
		The latest_active_records (lar) table is used to process the update file
		from Fivetran in history mode. We receive a file in which only updated
		columns are provided, so we need to "manually" fetch the values for the
		remaining columns to be able to insert a new valid row with all the right
		columns values. As this uses type 2 slowly changing dimensions, i.e. insert
		a new row on updates, we cannot use UPDATE x SET y = value, as this updates
		in place.
		*/
		lar_table_name = sql_generator->create_latest_active_records_table(con, table_name);

		// delete overlapping records
		for (auto& filename : request->earliest_start_files()) {
			logger.info("Processing earliest start file " + filename);
			// "This file contains a single record for each primary key in the
			// incoming batch, with the earliest _fivetran_start"
			std::vector<column_def> earliest_start_cols;
			earliest_start_cols.reserve(columns_pk.size() + 1);
			for (const auto& col : columns_pk) {
				earliest_start_cols.push_back(*col);
			}
			earliest_start_cols.push_back({.name = "_fivetran_start", .type = duckdb::LogicalTypeId::TIMESTAMP_TZ});

			const std::string decryption_key =
			    get_decryption_key(filename, request->keys(), request->file_params().encryption());
			IngestProperties props {.filename = filename,
			                        .decryption_key = decryption_key,
			                        .columns = earliest_start_cols,
			                        .null_value = request->file_params().null_string(),
			                        .allow_unmodified_string = false,
			                        .max_record_size = max_record_size};

			csv_processor::ProcessFile(con, props, logger, [&](const std::string& staging_table_name) {
				sql_generator->deactivate_historical_records(con, table_name, staging_table_name, lar_table_name,
				                                             columns_pk);
			});
		}

		for (auto& filename : request->update_files()) {
			logger.info("update file " + filename);
			const std::string decryption_key =
			    get_decryption_key(filename, request->keys(), request->file_params().encryption());
			IngestProperties props {.filename = filename,
			                        .decryption_key = decryption_key,
			                        .columns = cols,
			                        .null_value = request->file_params().null_string(),
			                        .allow_unmodified_string = true,
			                        .max_record_size = max_record_size};

			csv_processor::ProcessFile(con, props, logger, [&](const std::string& staging_table_name) {
				sql_generator->add_partial_historical_values(con, table_name, staging_table_name, lar_table_name,
				                                             columns_pk, columns_regular,
				                                             request->file_params().unmodified_string());
			});
		}

		// The following functions do not need the LAR table
		sql_generator->drop_latest_active_records_table(con, lar_table_name);

		// upsert files
		for (auto& filename : request->replace_files()) {
			logger.info("replace/upsert file " + filename);
			const std::string decryption_key =
			    get_decryption_key(filename, request->keys(), request->file_params().encryption());
			IngestProperties props {.filename = filename,
			                        .decryption_key = decryption_key,
			                        .columns = cols,
			                        .null_value = request->file_params().null_string(),
			                        .allow_unmodified_string = false,
			                        .max_record_size = max_record_size};

			csv_processor::ProcessFile(con, props, logger, [&](const std::string& staging_table_name) {
				sql_generator->insert(con, table_name, staging_table_name, columns_pk, columns_regular);
			});
		}

		for (auto& filename : request->delete_files()) {
			logger.info("delete file " + filename);
			// Fivetran delete files won't contain all the columns in the request
			// proto. Only primary keys and _fivetran_end are useful for the soft
			// delete. _fivetran_start is not present in delete files despite being a
			// primary key.
			std::vector<column_def> cols_to_read;
			cols_to_read.reserve(columns_pk.size() + 1);
			for (const auto& col : cols) {
				if ((col.primary_key && col.name != "_fivetran_start") || col.name == "_fivetran_end") {
					cols_to_read.push_back(col);
				}
			}

			const std::string decryption_key =
			    get_decryption_key(filename, request->keys(), request->file_params().encryption());
			IngestProperties props {.filename = filename,
			                        .decryption_key = decryption_key,
			                        .columns = cols_to_read,
			                        .null_value = request->file_params().null_string(),
			                        .allow_unmodified_string = false,
			                        .max_record_size = max_record_size};

			csv_processor::ProcessFile(con, props, logger, [&](const std::string& staging_table_name) {
				sql_generator->delete_historical_rows(con, table_name, staging_table_name, columns_pk);
			});
		}

	} catch (const md_error::RecoverableError& mde) {
		// Clean up bookkeeping table. The function uses IF EXISTS. Ignore any
		// errors here.
		sql_generator->drop_latest_active_records_table(con, lar_table_name);

		auto const msg = "WriteHistoryBatch endpoint failed for schema <" + request->schema_name() + ">, table <" +
		                 request->table().name() + ">: " + std::string(mde.what());
		response->mutable_task()->set_message(mde.what());
		return ::grpc::Status::OK;
	} catch (const std::exception& ex) {
		const std::string error_prefix = "WriteHistoryBatch endpoint failed for schema <" + request->schema_name() +
		                                 ">, table <" + request->table().name() + ">: ";
		const auto msg = error_prefix + ex.what();
		logger.severe(msg);

		// Clean up bookkeeping table. The function uses IF EXISTS. Ignore any
		// errors here.
		sql_generator->drop_latest_active_records_table(con, lar_table_name);

		response->mutable_task()->set_message(msg);
		return create_grpc_status_from_exception(ex, error_prefix);
	}

	return ::grpc::Status::OK;
}

std::string extract_readable_error(const std::exception& ex) {
	// DuckDB errors are JSON strings. Converting it to ErrorData to extract the
	// message.
	duckdb::ErrorData error(ex.what());
	std::string error_message = error.RawMessage();

	// Errors thrown in the initialization function are very verbose. Example:
	// Invalid Input Error: Initialization function "motherduck_duckdb_cpp_init"
	// from file "motherduck.duckdb_extension" threw an exception: "Failed to
	// attach 'my_db': no database/share named 'my_db' found". We are only
	// interested in the last part.
	const std::string boilerplate = "Initialization function \"motherduck_";
	if (error_message.find(boilerplate) != std::string::npos) {
		const std::string search_string = "threw an exception: ";
		const auto pos = error_message.find(search_string);
		if (pos != std::string::npos) {
			error_message = "Connection to MotherDuck failed: " + error_message.substr(pos + search_string.length());
		}
	}

	const std::string not_found_error_substr = "no database/share named";
	if (error_message.find(not_found_error_substr) != std::string::npos) {
		// Remove the quotation mark at the end
		error_message = error_message.substr(0, error_message.length() - 1);
		// Full error: "no database/share named 'my_db' found. Create it first in
		// your MotherDuck account."
		error_message += ". Create it first in your MotherDuck account.\"";
	}

	return error_message;
}

std::string get_migration_schema_name(const fivetran_sdk::v2::MigrationDetails& details) {
	const std::string& schema_name = details.schema();
	if (schema_name.empty()) {
		return "main";
	}
	return schema_name;
}

grpc::Status DestinationSdkImpl::Migrate(::grpc::ServerContext*, const ::fivetran_sdk::v2::MigrateRequest* request,
                                         ::fivetran_sdk::v2::MigrateResponse* response) {
	std::optional<RequestContext> ctx;
	try {
		ctx.emplace("Migrate", connection_factory, request->configuration());
	} catch (const std::exception& e) {
		return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
	}
	auto& con = ctx->GetConnection();
	auto& logger = ctx->GetLogger();

	try {
		const auto& details = request->details();
		const std::string schema_name = get_migration_schema_name(details);
		const std::string& table_name = details.table();

		if (table_name.empty()) {
			throw std::invalid_argument("Table name cannot be empty");
		}

		const std::string db_name = config::find_property(request->configuration(), config::PROP_DATABASE);
		auto sql_generator = std::make_unique<MdSqlGenerator>(logger);

		table_def table {db_name, schema_name, table_name};
		logger.info("Endpoint <Migrate>: schema <" + schema_name + ">, table <" + table_name + ">");

		switch (details.operation_case()) {
		case fivetran_sdk::v2::MigrationDetails::kDrop: {
			const auto& drop = details.drop();
			switch (drop.entity_case()) {
			case fivetran_sdk::v2::DropOperation::EntityCase::kDropTable: {
				logger.info("Endpoint <Migrate>: DROP_TABLE");
				sql_generator->drop_table(con, table, "drop_table");
				break;
			}
			case fivetran_sdk::v2::DropOperation::EntityCase::kDropColumnInHistoryMode: {
				logger.info("Endpoint <Migrate>: DROP_COLUMN_IN_HISTORY_MODE");
				const auto& drop_col = drop.drop_column_in_history_mode();
				sql_generator->drop_column_in_history_mode(con, table, drop_col.column(),
				                                           drop_col.operation_timestamp());
				break;
			}
			default: {
				logger.warning("Endpoint <Migrate>: received unsupported drop mode "
				               "operation type");
				response->set_unsupported(true);
				return ::grpc::Status::OK;
			}
			}
			break;
		}
		case fivetran_sdk::v2::MigrationDetails::kCopy: {
			const auto& copy = details.copy();
			switch (copy.entity_case()) {
			case fivetran_sdk::v2::CopyOperation::EntityCase::kCopyTable: {
				logger.info("Endpoint <Migrate>: COPY_TABLE");
				const auto& copy_table = copy.copy_table();
				table_def from_table {db_name, schema_name, copy_table.from_table()};
				table_def to_table {db_name, schema_name, copy_table.to_table()};
				sql_generator->copy_table(con, from_table, to_table, "copy_table");
				break;
			}
			case fivetran_sdk::v2::CopyOperation::EntityCase::kCopyColumn: {
				logger.info("Endpoint <Migrate>: COPY_COLUMN");
				const auto& copy_col = copy.copy_column();
				if (is_fivetran_system_column(copy_col.to_column())) {
					throw std::invalid_argument("Cannot copy column to reserved name <" + copy_col.to_column() +
					                            ">. Please contact Fivetran support.");
				}

				sql_generator->copy_column(con, table, copy_col.from_column(), copy_col.to_column());
				break;
			}
			case fivetran_sdk::v2::CopyOperation::EntityCase::kCopyTableToHistoryMode: {
				logger.info("Endpoint <Migrate>: COPY_TABLE_TO_HISTORY_MODE");
				const auto& copy_hist = copy.copy_table_to_history_mode();
				table_def from_table {db_name, schema_name, copy_hist.from_table()};
				table_def to_table {db_name, schema_name, copy_hist.to_table()};
				sql_generator->copy_table_to_history_mode(con, from_table, to_table, copy_hist.soft_deleted_column());
				break;
			}
			default: {
				response->set_unsupported(true);
				return ::grpc::Status::OK;
			}
			}
			break;
		}
		case fivetran_sdk::v2::MigrationDetails::kRename: {
			const auto& rename = details.rename();
			switch (rename.entity_case()) {
			case fivetran_sdk::v2::RenameOperation::EntityCase::kRenameTable: {
				logger.info("Endpoint <Migrate>: RENAME_TABLE");
				const auto& rename_tbl = rename.rename_table();
				table_def from_table {db_name, schema_name, rename_tbl.from_table()};
				sql_generator->rename_table(con, from_table, rename_tbl.to_table(), "rename_table");
				break;
			}
			case fivetran_sdk::v2::RenameOperation::EntityCase::kRenameColumn: {
				logger.info("Endpoint <Migrate>: RENAME_COLUMN");
				const auto& rename_col = rename.rename_column();

				if (is_fivetran_system_column(rename_col.to_column())) {
					throw std::invalid_argument("Cannot rename column to reserved name <" + rename_col.to_column() +
					                            ">. Please contact Fivetran support.");
				}
				sql_generator->rename_column(con, table, rename_col.from_column(), rename_col.to_column());
				break;
			}
			default: {
				response->set_unsupported(true);
				return ::grpc::Status::OK;
			}
			}
			break;
		}
		case fivetran_sdk::v2::MigrationDetails::kAdd: {
			const auto& add = details.add();
			switch (add.entity_case()) {
			case fivetran_sdk::v2::AddOperation::EntityCase::kAddColumnWithDefaultValue: {
				logger.info("Endpoint <Migrate>: ADD_COLUMN_WITH_DEFAULT_VALUE");
				const auto& add_col = add.add_column_with_default_value();

				column_def column {
				    .name = add_col.column(),
				    .type = get_duckdb_type(add_col.column_type()),
				    .column_default = add_col.default_value(),
				    .primary_key = false,
				};

				if (is_fivetran_system_column(column.name)) {
					throw std::invalid_argument("Cannot add column with reserved name <" + column.name +
					                            ">. Please contact Fivetran support.");
				}

				sql_generator->add_column(con, table, column, "add_column");
				break;
			}
			case fivetran_sdk::v2::AddOperation::EntityCase::kAddColumnInHistoryMode: {
				logger.info("Endpoint <Migrate>: ADD_COLUMN_IN_HISTORY_MODE");
				const auto& add_col = add.add_column_in_history_mode();

				// The default value should not be a DDL level default, because NULLs in
				// history mode can signify the column not existing in the past.
				column_def col {
				    .name = add_col.column(),
				    .type = get_duckdb_type(add_col.column_type()),
				    .primary_key = false,
				};
				sql_generator->add_column_in_history_mode(con, table, col, add_col.operation_timestamp(),
				                                          add_col.default_value());
				break;
			}
			default: {
				response->set_unsupported(true);
				return ::grpc::Status::OK;
			}
			}
			break;
		}
		case fivetran_sdk::v2::MigrationDetails::kUpdateColumnValue: {
			logger.info("Endpoint <Migrate>: UpdateColumnValueOperation");
			const auto& update = details.update_column_value();
			sql_generator->update_column_value(con, table, update.column(), update.value());
			break;
		}
		case fivetran_sdk::v2::MigrationDetails::kTableSyncModeMigration: {
			const auto& sync_mode = details.table_sync_mode_migration();
			const std::string soft_deleted_column =
			    sync_mode.has_soft_deleted_column() ? sync_mode.soft_deleted_column() : "_fivetran_deleted";
			const bool keep_deleted_rows = sync_mode.has_keep_deleted_rows() ? sync_mode.keep_deleted_rows() : false;

			switch (sync_mode.type()) {
			// Note: officially live mode is not supported yet for the partner SDK.
			// Hence, the LIVE_TO_* and *_TO_LIVE are not yet expected to be sent out,
			// and we could expect changes to the docs/spec on live mode in the
			// future.
			case fivetran_sdk::v2::SOFT_DELETE_TO_LIVE:
				logger.info("Endpoint <Migrate>: SOFT_DELETE_TO_LIVE");
				sql_generator->migrate_soft_delete_to_live(con, table, soft_deleted_column);
				break;
			case fivetran_sdk::v2::SOFT_DELETE_TO_HISTORY:
				logger.info("Endpoint <Migrate>: SOFT_DELETE_TO_HISTORY");
				sql_generator->migrate_soft_delete_to_history(con, table, soft_deleted_column);
				break;
			case fivetran_sdk::v2::HISTORY_TO_SOFT_DELETE:
				logger.info("Endpoint <Migrate>: HISTORY_TO_SOFT_DELETE");
				sql_generator->migrate_history_to_soft_delete(con, table, soft_deleted_column);
				break;
			case fivetran_sdk::v2::HISTORY_TO_LIVE:
				logger.info("Endpoint <Migrate>: HISTORY_TO_LIVE");
				sql_generator->migrate_history_to_live(con, table, keep_deleted_rows);
				break;
			case fivetran_sdk::v2::LIVE_TO_SOFT_DELETE:
				logger.info("Endpoint <Migrate>: LIVE_TO_SOFT_DELETE");
				sql_generator->migrate_live_to_soft_delete(con, table, soft_deleted_column);
				break;
			case fivetran_sdk::v2::LIVE_TO_HISTORY:
				logger.info("Endpoint <Migrate>: LIVE_TO_HISTORY");
				sql_generator->migrate_live_to_history(con, table);
				break;
			default:
				response->set_unsupported(true);
				logger.warning("Endpoint <Migrate>: unsupported sync mode type");
				return ::grpc::Status::OK;
			}
			break;
		}
		default:
			logger.warning("Endpoint <Migrate>: Unknown operation type");
			response->set_unsupported(true);
			return ::grpc::Status::OK;
		}

		response->set_success(true);
	} catch (const std::exception& e) {
		const std::string schema = request->details().schema();
		const std::string table = request->details().table();
		logger.severe("Migrate endpoint failed for schema <" + schema + ">, table <" + table +
		              ">: " + std::string(e.what()));
		response->mutable_task()->set_message(e.what());
		return ::grpc::Status(::grpc::StatusCode::INTERNAL, e.what());
	}

	return ::grpc::Status(::grpc::StatusCode::OK, "");
}

grpc::Status DestinationSdkImpl::Test(::grpc::ServerContext*, const ::fivetran_sdk::v2::TestRequest* request,
                                      ::fivetran_sdk::v2::TestResponse* response) {
	const std::string test_name = request->name();
	const std::string error_prefix = "Test <" + test_name + "> failed: ";
	const auto user_config = request->configuration();

	try {
		// This constructor already loads the extension and connects to MotherDuck.
		// If this fails, we catch the exception and rewrite it a bit to make
		// it more actionable.
		RequestContext ctx("Test", connection_factory, request->configuration());

		auto test_result = config_tester::run_test(test_name, ctx.GetConnection(), request->configuration());
		if (test_result.success) {
			response->set_success(true);
		} else {
			response->set_failure(error_prefix + test_result.failure_message);
		}
	} catch (const std::exception& ex) {
		const auto error_message = extract_readable_error(ex);
		response->set_failure(error_prefix + error_message);
	}
	return ::grpc::Status::OK;
}
