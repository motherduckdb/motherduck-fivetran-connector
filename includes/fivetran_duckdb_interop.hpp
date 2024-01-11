#include "destination_sdk.grpc.pb.h"
#include "duckdb.hpp"

fivetran_sdk::DataType get_fivetran_type(const duckdb::LogicalTypeId &duckdb_type);

duckdb::LogicalTypeId get_duckdb_type(const fivetran_sdk::DataType &fivetranType);