#include "duckdb.hpp"
#include "motherduck_destination_server.hpp"
#include <csignal>
#include <execinfo.h>
#include <grpcpp/grpcpp.h>
#include <string>

void RunServer(const std::string &port) {
  std::string server_address = "0.0.0.0:" + port;
  DestinationSdkImpl service;

  grpc::EnableDefaultHealthCheckService(true);

  grpc::ServerBuilder builder;

  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
}

void download_motherduck_extension() {
  // create an in-memory DuckDB instance
  duckdb::DuckDB db;
  duckdb::Connection con(db);
  {
    auto result = con.Query("INSTALL motherduck");
    if (result->HasError()) {
      throw std::runtime_error("Could not install motherduck extension prior "
                               "to gRPC server startup");
    }
  }
  {
    auto result = con.Query("LOAD motherduck");
    if (result->HasError()) {
      throw std::runtime_error(
          "Could not load motherduck extension prior to gRPC server startup");
    }
  }
}

void logCrash(int sig) {
  void *array[512];
  size_t size = backtrace(array, 512);
  char **strings = backtrace_symbols(array, size);

  std::cerr << "Crash signal " << sig << std::endl;
  std::cerr << "Stack trace: " << std::endl;

  for (size_t i = 0; i < size; i++) {
    std::cerr << strings[i] << std::endl;
  }

  free(strings);
  std::exit(sig);
}

int main(int argc, char **argv) {
  std::signal(SIGSEGV, logCrash);
  std::signal(SIGABRT, logCrash);

  std::string port = "50052";
  for (auto i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--port") == 0) {
      if (i + 1 >= argc) {
        throw std::runtime_error("Please provide a port number.\nUsage: "
                                 "motherduck_destination [--port <PORT>]");
      }
      port = argv[i + 1];
    }
    std::cout << "argument: " << argv[i] << std::endl;
  }

  download_motherduck_extension();
  RunServer(port);
  return 0;
}
