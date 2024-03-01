#include <fivetran_duckdb_interop.hpp>

using duckdb::LogicalTypeId;

fivetran_sdk::DataType get_fivetran_type(const LogicalTypeId &duckdb_type) {
  switch (duckdb_type) {
  case LogicalTypeId::BOOLEAN:
    return fivetran_sdk::BOOLEAN;
  case LogicalTypeId::SMALLINT:
    return fivetran_sdk::SHORT;
  case LogicalTypeId::INTEGER:
    return fivetran_sdk::INT;
  case LogicalTypeId::BIGINT:
    return fivetran_sdk::LONG;
  case LogicalTypeId::FLOAT:
    return fivetran_sdk::FLOAT;
  case LogicalTypeId::DOUBLE:
    return fivetran_sdk::DOUBLE;
  case LogicalTypeId::DATE:
    return fivetran_sdk::NAIVE_DATE;
  case LogicalTypeId::TIMESTAMP:
    return fivetran_sdk::UTC_DATETIME;
  case LogicalTypeId::DECIMAL:
    return fivetran_sdk::DECIMAL;
  case LogicalTypeId::BIT:
    return fivetran_sdk::BINARY;
  case LogicalTypeId::VARCHAR:
    return fivetran_sdk::STRING;
  default:
    return fivetran_sdk::UNSPECIFIED;
  }
}

LogicalTypeId get_duckdb_type(const fivetran_sdk::DataType &fivetranType) {
  switch (fivetranType) {
  case fivetran_sdk::BOOLEAN:
    return LogicalTypeId::BOOLEAN;
  case fivetran_sdk::SHORT:
    return LogicalTypeId::SMALLINT;
  case fivetran_sdk::INT:
    return LogicalTypeId::INTEGER;
  case fivetran_sdk::LONG:
    return LogicalTypeId::BIGINT;
  case fivetran_sdk::FLOAT:
    return LogicalTypeId::FLOAT;
  case fivetran_sdk::DOUBLE:
    return LogicalTypeId::DOUBLE;
  case fivetran_sdk::NAIVE_DATE:
    return LogicalTypeId::DATE;
  case fivetran_sdk::NAIVE_DATETIME:
    return LogicalTypeId::TIMESTAMP;
  case fivetran_sdk::UTC_DATETIME:
    return LogicalTypeId::TIMESTAMP; // TBD: this is pretty definitely wrong;
                                     // needs to be with timezone
  case fivetran_sdk::DECIMAL:
    return LogicalTypeId::DECIMAL;
  case fivetran_sdk::BINARY:
    return LogicalTypeId::BIT;
  case fivetran_sdk::STRING:
    return LogicalTypeId::VARCHAR;
  case fivetran_sdk::JSON:
    return LogicalTypeId::
        VARCHAR; // https://github.com/MotherDuck-Open-Source/motherduck-fivetran-connector/issues/22
  default:
    return LogicalTypeId::INVALID;
  }
}