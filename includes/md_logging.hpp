#pragma once

#include "logging_sink.grpc.pb.h"
#include <iostream>
#include <map>

namespace mdlog {

class MdLog {
public:
  void log(const std::string &level, const std::string &message);

  void info(const std::string &message);

  void warning(const std::string &message);

  void severe(const std::string &message);

  void set_duckdb_id(const std::string &duckdb_id_);

  void
  set_remote_sink(const std::string &token_,
                  std::shared_ptr<logging_sink::LoggingSink::Stub> &client_);

private:
  std::string duckdb_id = "none";
  std::string token;
  std::shared_ptr<logging_sink::LoggingSink::Stub> client = nullptr;
};

std::string escape_char(const std::string &str, const char &c);

} // namespace mdlog
