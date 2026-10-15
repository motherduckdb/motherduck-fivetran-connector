#include "crash_handler.hpp"
#include "extension_helper.hpp"
#include "motherduck_destination_server.hpp"

#include <cstdlib>
#include <grpcpp/grpcpp.h>
#include <string>

void RunServer(const std::string& port) {
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

int main(const int argc, char** argv) {
	// Pin the MotherDuck extension to the build carrying the catalog-snapshot trim-race
	// fix. Must be set before preload_extensions() loads the wrapper, which reads this
	// env var to resolve which extension version to download. Set in-process because the
	// connector runs in Fivetran's harness, not our image, so a Dockerfile ENV would not
	// reach it.
	setenv("motherduck_ext_version", "v1-5-5-2026-09-flo-catalog-snapshot-trim-ra-05b44b64c3", 1);

	crash_handler::Install();

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
