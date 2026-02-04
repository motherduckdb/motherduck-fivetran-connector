#include "extension_helper.hpp"
#include "motherduck_destination_server.hpp"
#include "stacktrace.hpp"

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

void logCrash(int sig) {
	std::cerr << "Crash signal " << sig << std::endl;
	auto trace = duckdb_copy::StackTrace::GetStackTrace();
	std::cerr << "Stack Trace:" << trace << std::endl;
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

	preload_extensions();
	RunServer(port);
	return 0;
}
