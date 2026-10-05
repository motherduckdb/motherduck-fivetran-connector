#include "crash_handler.hpp"

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <csignal>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

using Catch::Matchers::ContainsSubstring;

namespace {

struct ChildDeath {
	int signal;
	std::string stderr_output;
};

// Runs `die` in a forked child with the crash handler installed, the way motherduck_destination's main() does,
// and returns the signal that killed the child together with everything it wrote to stderr.
ChildDeath die_with_crash_handler(void (*die)()) {
	int fds[2];
	REQUIRE(pipe(fds) == 0);
	const pid_t pid = fork();
	REQUIRE(pid >= 0);
	if (pid == 0) {
		close(fds[0]);
		dup2(fds[1], STDERR_FILENO);
		alarm(10); // if the handler deadlocks, die by SIGALRM instead of hanging the test run
		crash_handler::Install();
		die();
		_exit(0);
	}
	close(fds[1]);

	std::string output;
	char buffer[4096];
	for (ssize_t n; (n = read(fds[0], buffer, sizeof(buffer))) > 0;) {
		output.append(buffer, static_cast<size_t>(n));
	}
	close(fds[0]);

	int status = 0;
	REQUIRE(waitpid(pid, &status, 0) == pid);
	REQUIRE(WIFSIGNALED(status));
	return {WTERMSIG(status), output};
}

// backtrace_symbols_fd() writes one line per frame between the "frames=N" line and the end banner,
// so the reported count must match the number of lines actually printed.
void check_backtrace(const std::string& output, const int signal) {
	REQUIRE_THAT(output, ContainsSubstring("=== SIGSEGV or SIGABRT ===\nsignal=" + std::to_string(signal) + "\n"));
	REQUIRE_THAT(output, ContainsSubstring("\nframes="));
	REQUIRE_THAT(output, ContainsSubstring("=== end of stack trace ===\n"));

	const auto frames_pos = output.find("\nframes=");
	const int reported_frames = std::stoi(output.substr(frames_pos + 8));
	const auto symbols_begin = output.find('\n', frames_pos + 1) + 1;
	const auto symbols_end = output.find("=== end of stack trace ===");
	const auto symbols = output.substr(symbols_begin, symbols_end - symbols_begin);
	const auto printed_lines = std::count(symbols.begin(), symbols.end(), '\n');

	CHECK(reported_frames > 1);
	CHECK(printed_lines == reported_frames);
}

[[noreturn]] void throw_boom() {
	throw std::runtime_error("boom");
}

struct ThrowsInDestructor {
	~ThrowsInDestructor() {
		throw_boom();
	}
};

} // namespace

TEST_CASE("Crash handler", "[crash_handler]") {
	SECTION("segfault prints banner, signal and backtrace") {
		const auto death = die_with_crash_handler([] {
			volatile int* null = nullptr;
			*null = 42;
		});
		CHECK(death.signal == SIGSEGV);
		check_backtrace(death.stderr_output, SIGSEGV);
	}

	SECTION("exception escaping a destructor (std::terminate) prints banner, signal and backtrace") {
		const auto death = die_with_crash_handler([] { ThrowsInDestructor t; });
		CHECK(death.signal == SIGABRT);
		CHECK_THAT(death.stderr_output, ContainsSubstring("boom"));
		check_backtrace(death.stderr_output, SIGABRT);
	}

	SECTION("SIGTERM is reported as orderly shutdown without a backtrace") {
		const auto death = die_with_crash_handler([] { raise(SIGTERM); });
		CHECK(death.signal == SIGTERM);
		CHECK_THAT(death.stderr_output,
		           ContainsSubstring("=== terminated by signal 15 (orderly shutdown, not a crash) ===\n"));
		CHECK_THAT(death.stderr_output, !ContainsSubstring("Stack trace"));
	}
}
