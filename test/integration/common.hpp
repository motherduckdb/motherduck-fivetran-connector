#pragma once

#include <string>
#include "duckdb.hpp"
#include "motherduck_destination_server.hpp"
#include "extension_helper.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>

inline bool NO_FAIL(
    const duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result) {
  if (result->HasError()) {
    fprintf(stderr, "Query failed with message: %s\n",
            result->GetError().c_str());
  }
  return !result->HasError();
}

inline bool NO_FAIL(const grpc::Status &status) {
  if (!status.ok()) {
    UNSCOPED_INFO("Query failed with message: " + status.error_message());
  }
  return status.ok();
}

inline bool REQUIRE_FAIL(const grpc::Status &status,
                  const std::string &expected_error) {
  if (!status.ok()) {
    REQUIRE(status.error_message() == expected_error);
    return true;
  }
  return false;
}

inline bool REQUIRE_FAIL(const grpc::Status &status,
                  const Catch::Matchers::StringMatcherBase &matcher) {
  if (!status.ok()) {
    REQUIRE_THAT(status.error_message(), matcher);
    return true;
  }
  return false;
}

#define REQUIRE_NO_FAIL(result) REQUIRE(NO_FAIL((result)))

class testRunListener : public Catch::EventListenerBase {
public:
  using EventListenerBase::EventListenerBase;

  void testRunStarting(Catch::TestRunInfo const &) override {
    preload_extensions();
  }
};

CATCH_REGISTER_LISTENER(testRunListener)

std::unique_ptr<duckdb::Connection> get_test_connection(const std::string &token);

void check_row(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &res,
               idx_t row, std::initializer_list<duckdb::Value> expected);

template <typename T>
void define_test_table(T &request, const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
  auto col1 = request.mutable_table()->add_columns();
  col1->set_name("id");
  col1->set_type(::fivetran_sdk::v2::DataType::INT);
  col1->set_primary_key(true);

  auto col2 = request.mutable_table()->add_columns();
  col2->set_name("title");
  col2->set_type(::fivetran_sdk::v2::DataType::STRING);

  auto col3 = request.mutable_table()->add_columns();
  col3->set_name("magic_number");
  col3->set_type(::fivetran_sdk::v2::DataType::INT);

  auto col4 = request.mutable_table()->add_columns();
  col4->set_name("_fivetran_deleted");
  col4->set_type(::fivetran_sdk::v2::DataType::BOOLEAN);

  auto col5 = request.mutable_table()->add_columns();
  col5->set_name("_fivetran_synced");
  col5->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);
}

template <typename T>
void define_history_test_table(T &request, const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
  auto col1 = request.mutable_table()->add_columns();
  col1->set_name("id");
  col1->set_type(::fivetran_sdk::v2::DataType::INT);
  col1->set_primary_key(true);

  auto col2 = request.mutable_table()->add_columns();
  col2->set_name("title");
  col2->set_type(::fivetran_sdk::v2::DataType::STRING);

  auto col3 = request.mutable_table()->add_columns();
  col3->set_name("magic_number");
  col3->set_type(::fivetran_sdk::v2::DataType::INT);

  auto col4 = request.mutable_table()->add_columns();
  col4->set_name("_fivetran_deleted");
  col4->set_type(::fivetran_sdk::v2::DataType::BOOLEAN);

  auto col5 = request.mutable_table()->add_columns();
  col5->set_name("_fivetran_synced");
  col5->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);

  auto col6 = request.mutable_table()->add_columns();
  col6->set_name("_fivetran_active");
  col6->set_type(::fivetran_sdk::v2::DataType::BOOLEAN);

  auto col7 = request.mutable_table()->add_columns();
  col7->set_name("_fivetran_start");
  col7->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);
  col7->set_primary_key(true);

  auto col8 = request.mutable_table()->add_columns();
  col8->set_name("_fivetran_end");
  col8->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);
}

template <typename T>
void define_transaction_test_table(T &request, const std::string &table_name) {
  request.mutable_table()->set_name(table_name);

  auto col1 = request.mutable_table()->add_columns();
  col1->set_name("amount");
  col1->set_type(::fivetran_sdk::v2::DataType::INT);

  auto col2 = request.mutable_table()->add_columns();
  col2->set_name("_fivetran_synced");
  col2->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);

  auto col3 = request.mutable_table()->add_columns();
  col3->set_name("id");
  col3->set_type(::fivetran_sdk::v2::DataType::INT);
  col3->set_primary_key(true);

  auto col4 = request.mutable_table()->add_columns();
  col4->set_name("desc");
  col4->set_type(::fivetran_sdk::v2::DataType::STRING);
}

template <typename T>
void define_transaction_history_test_table(T &request,
                                           const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
  auto col1 = request.mutable_table()->add_columns();
  col1->set_name("amount");
  col1->set_type(::fivetran_sdk::v2::DataType::INT);

  auto col2 = request.mutable_table()->add_columns();
  col2->set_name("_fivetran_synced");
  col2->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);

  auto col3 = request.mutable_table()->add_columns();
  col3->set_name("_fivetran_end");
  col3->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);

  auto col4 = request.mutable_table()->add_columns();
  col4->set_name("_fivetran_active");
  col4->set_type(::fivetran_sdk::v2::DataType::BOOLEAN);

  auto col5 = request.mutable_table()->add_columns();
  col5->set_name("desc");
  col5->set_type(::fivetran_sdk::v2::DataType::STRING);

  auto col6 = request.mutable_table()->add_columns();
  col6->set_name("_fivetran_start");
  col6->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);
  col6->set_primary_key(true);

  auto col7 = request.mutable_table()->add_columns();
  col7->set_name("id");
  col7->set_type(::fivetran_sdk::v2::DataType::INT);
  col7->set_primary_key(true);
}

template <typename T>
void define_test_multikey_table(T &request, const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
  auto col1 = request.mutable_table()->add_columns();
  col1->set_name("id1");
  col1->set_type(::fivetran_sdk::v2::DataType::INT);
  col1->set_primary_key(true);

  auto col2 = request.mutable_table()->add_columns();
  col2->set_name("id2");
  col2->set_type(::fivetran_sdk::v2::DataType::INT);
  col2->set_primary_key(true);

  auto col3 = request.mutable_table()->add_columns();
  col3->set_name("text");
  col3->set_type(::fivetran_sdk::v2::DataType::STRING);

  auto col4 = request.mutable_table()->add_columns();
  col4->set_name("_fivetran_deleted");
  col4->set_type(::fivetran_sdk::v2::DataType::BOOLEAN);

  auto col5 = request.mutable_table()->add_columns();
  col5->set_name("_fivetran_synced");
  col5->set_type(::fivetran_sdk::v2::DataType::UTC_DATETIME);
}

/* compression and encryption are off/none by default */
template <typename T>
void set_up_plain_write_request(T &request, const std::string &token,
                                const std::string db_name) {
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] = db_name;
}

template <typename T>
void add_config(T &request, const std::string &token,
                const std::string &database, const std::string &table) {
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] = database;
  request.mutable_table()->set_name(table);
}

template <typename T>
void add_config(T &request, const std::string &token,
                const std::string &database) {
  (*request.mutable_configuration())["motherduck_token"] = token;
  (*request.mutable_configuration())["motherduck_database"] = database;
}

template <typename T>
void add_col(T &request, const std::string &name,
             ::fivetran_sdk::v2::DataType type, bool is_primary_key) {
  auto col = request.mutable_table()->add_columns();
  col->set_name(name);
  col->set_type(type);
  col->set_primary_key(is_primary_key);
}

template <typename T>
void add_decimal_col(T &request, const std::string &name, bool is_primary_key,
                     std::uint32_t precision, std::uint32_t scale) {
  auto col = request.mutable_table()->add_columns();
  col->set_name(name);
  col->set_type(::fivetran_sdk::v2::DataType::DECIMAL);
  col->set_primary_key(is_primary_key);
  col->mutable_params()->mutable_decimal()->set_precision(precision);
  col->mutable_params()->mutable_decimal()->set_scale(scale);
}

// Same columns as define_history_test_table but in a DIFFERENT order.
// Used to test that INSERT statements use explicit column lists.
template <typename T>
void define_history_test_table_reordered(T &request,
                                         const std::string &table_name) {
  request.mutable_table()->set_name(table_name);
  add_col(request, "_fivetran_end", ::fivetran_sdk::v2::DataType::UTC_DATETIME,
          false);
  add_col(request, "magic_number", ::fivetran_sdk::v2::DataType::INT, false);
  add_col(request, "_fivetran_active", ::fivetran_sdk::v2::DataType::BOOLEAN,
          false);
  add_col(request, "title", ::fivetran_sdk::v2::DataType::STRING, false);
  add_col(request, "_fivetran_synced",
          ::fivetran_sdk::v2::DataType::UTC_DATETIME, false);
  add_col(request, "_fivetran_start",
          ::fivetran_sdk::v2::DataType::UTC_DATETIME, true);
  add_col(request, "_fivetran_deleted", ::fivetran_sdk::v2::DataType::BOOLEAN,
          false);
  add_col(request, "id", ::fivetran_sdk::v2::DataType::INT, true);
}
