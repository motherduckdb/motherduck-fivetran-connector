#include "extension_helper.hpp"
#include "motherduck_destination_server.hpp"

#include <execinfo.h>
#include <grpcpp/grpcpp.h>
#include <signal.h>
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

void log_crash(const int sig) {
	constexpr char msg[] = "\n=== SIGSEGV or SIGABRT ===\nStack trace:\n";
	write(STDERR_FILENO, msg, sizeof(msg) - 1);

	// backtrace() and backtrace_symbols_fd() are not specified by POSIX
	// as async-signal-safe and may allocate internally, but they are commonly
	// used in crash handlers as a best-effort way to capture a stack trace.
	static constexpr int MAX_DEPTH = 120;
	void* callstack[MAX_DEPTH];
	const int num_frames = backtrace(callstack, MAX_DEPTH);
	backtrace_symbols_fd(callstack, num_frames, STDERR_FILENO);

	raise(sig);
}

int main(const int argc, char** argv) {
	struct sigaction sa = {};
	sigemptyset(&sa.sa_mask);
	sa.sa_handler = log_crash;
	// SA_RESETHAND resets the signal handler to the default. If log_crash itself segfaults, then the default handler
	// (coredump) is triggered. SA_NODEFER makes sure that if log_crash itself segfaults, the process dies immediately
	// instead of hanging until log_crash is finished.
	sa.sa_flags = static_cast<int>(SA_RESETHAND | SA_NODEFER);
	sigaction(SIGSEGV, &sa, nullptr);
	sigaction(SIGABRT, &sa, nullptr);

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
