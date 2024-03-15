#include "motherduck_destination_server.hpp"
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

int main(int argc, char **argv) {
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

  std::cout << "before run server" << std::endl;
  RunServer(port);
  return 0;
}
