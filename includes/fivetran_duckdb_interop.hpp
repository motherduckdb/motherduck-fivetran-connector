#include "destination_sdk.grpc.pb.h"
#include "duckdb.hpp"

fivetran_sdk::v2::DataType
get_fivetran_type(const duckdb::LogicalTypeId &duckdb_type);

duckdb::LogicalTypeId
get_duckdb_type(const fivetran_sdk::v2::DataType &fivetranType);

inline std::string
fivetran_type_to_duckdb_type_string(fivetran_sdk::v2::DataType type) {
  duckdb::LogicalTypeId duckdb_type = get_duckdb_type(type);
  return duckdb::EnumUtil::ToChars(duckdb_type);
}
