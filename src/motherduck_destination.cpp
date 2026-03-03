#include "extension_helper.hpp"
#include "motherduck_destination_server.hpp"

#include <csignal>
#include <execinfo.h>
#include <grpcpp/grpcpp.h>
#include <string>
#include <unistd.h>

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

void logCrash(const int sig) {
	constexpr char msg[] = "\n=== SIGSEGV or SIGABRT ===\nStack trace:\n";
	write(STDERR_FILENO, msg, sizeof(msg) - 1);

	// backtrace() and backtrace_symbols_fd() are not specified by POSIX
	// as async-signal-safe and may allocate internally, but they are commonly
	// used in crash handlers as a best-effort way to capture a stack trace.
	static constexpr int MAX_DEPTH = 120;
	void* callstack[MAX_DEPTH];
	const int num_frames = backtrace(callstack, MAX_DEPTH);
	backtrace_symbols_fd(callstack, num_frames, STDERR_FILENO);

	signal(sig, SIG_DFL);
	raise(sig);
}

int main(const int argc, char** argv) {
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
