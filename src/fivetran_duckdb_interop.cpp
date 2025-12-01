#include <fivetran_duckdb_interop.hpp>

using duckdb::LogicalTypeId;

fivetran_sdk::v2::DataType get_fivetran_type(const LogicalTypeId &duckdb_type) {
  switch (duckdb_type) {
  case LogicalTypeId::BOOLEAN:
    return fivetran_sdk::v2::BOOLEAN;
  case LogicalTypeId::SMALLINT:
    return fivetran_sdk::v2::SHORT;
  case LogicalTypeId::INTEGER:
    return fivetran_sdk::v2::INT;
  case LogicalTypeId::BIGINT:
    return fivetran_sdk::v2::LONG;
  case LogicalTypeId::FLOAT:
    return fivetran_sdk::v2::FLOAT;
  case LogicalTypeId::DOUBLE:
    return fivetran_sdk::v2::DOUBLE;
  case LogicalTypeId::DATE:
    return fivetran_sdk::v2::NAIVE_DATE;
  case LogicalTypeId::TIMESTAMP:
    return fivetran_sdk::v2::NAIVE_DATETIME;
  case LogicalTypeId::TIMESTAMP_TZ:
    return fivetran_sdk::v2::UTC_DATETIME;
  case LogicalTypeId::DECIMAL:
    return fivetran_sdk::v2::DECIMAL;
  case LogicalTypeId::BIT:
    return fivetran_sdk::v2::BINARY;
  case LogicalTypeId::VARCHAR:
    return fivetran_sdk::v2::STRING;
  default:
    return fivetran_sdk::v2::UNSPECIFIED;
  }
}

LogicalTypeId get_duckdb_type(const fivetran_sdk::v2::DataType &fivetranType) {
  switch (fivetranType) {
  case fivetran_sdk::v2::BOOLEAN:
    return LogicalTypeId::BOOLEAN;
  case fivetran_sdk::v2::SHORT:
    return LogicalTypeId::SMALLINT;
  case fivetran_sdk::v2::INT:
    return LogicalTypeId::INTEGER;
  case fivetran_sdk::v2::LONG:
    return LogicalTypeId::BIGINT;
  case fivetran_sdk::v2::FLOAT:
    return LogicalTypeId::FLOAT;
  case fivetran_sdk::v2::DOUBLE:
    return LogicalTypeId::DOUBLE;
  case fivetran_sdk::v2::NAIVE_DATE:
    return LogicalTypeId::DATE;
  case fivetran_sdk::v2::NAIVE_DATETIME:
    return LogicalTypeId::TIMESTAMP;
  case fivetran_sdk::v2::UTC_DATETIME:
    return LogicalTypeId::TIMESTAMP_TZ; // TODO: find format Fivetran sends;
                                        // make sure UTC included
  case fivetran_sdk::v2::DECIMAL:
    return LogicalTypeId::DECIMAL;
  case fivetran_sdk::v2::BINARY:
    return LogicalTypeId::BIT;
  case fivetran_sdk::v2::STRING:
    return LogicalTypeId::VARCHAR;
  case fivetran_sdk::v2::JSON:
    return LogicalTypeId::
        VARCHAR; // https://github.com/MotherDuck-Open-Source/motherduck-fivetran-connector/issues/22
  default:
    return LogicalTypeId::INVALID;
  }
}