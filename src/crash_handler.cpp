#include "crash_handler.hpp"

#include <execinfo.h>
#include <signal.h>
#include <unistd.h>

namespace crash_handler {

namespace {

// Everything a handler touches must be async-signal-safe: write(2) only, no
// allocation and no formatting through the standard library.

void write_all(const char* data, const size_t len) {
	size_t written = 0;
	while (written < len) {
		const ssize_t n = write(STDERR_FILENO, data + written, len - written);
		if (n <= 0) {
			return;
		}
		written += static_cast<size_t>(n);
	}
}

template <size_t N>
void write_literal(const char (&literal)[N]) {
	write_all(literal, N - 1);
}

void write_unsigned(unsigned long value) {
	// Neither printf nor std::to_string may be used here, so the digits are
	// produced by hand: written right-aligned into the buffer, then emitted from
	// the first one. The do/while is what makes zero render as "0" rather than
	// as nothing at all.
	constexpr size_t BUFFER_SIZE = 24; // enough for any 64-bit value in decimal
	char buffer[BUFFER_SIZE];
	size_t offset = BUFFER_SIZE;
	do {
		buffer[--offset] = static_cast<char>('0' + (value % 10));
		value /= 10;
	} while (value > 0 && offset > 0);

	write_all(buffer + offset, BUFFER_SIZE - offset);
}

// Pre-allocated so the handler needs no stack space for it, which also gives it
// a chance of reporting a stack overflow (delivered as SIGSEGV).
constexpr int MAX_DEPTH = 120;
void* crash_callstack[MAX_DEPTH];

void log_crash(const int sig) {
	write_literal("\n=== SIGSEGV or SIGABRT ===\n");
	write_literal("signal=");
	write_unsigned(static_cast<unsigned long>(sig));
	write_literal("\nStack trace:\n");

	// backtrace() and backtrace_symbols_fd() are not specified by POSIX as
	// async-signal-safe and may allocate internally, but they are commonly used
	// in crash handlers as a best-effort way to capture a stack trace. Install()
	// warms them up so the first call is not made from here.
	const int num_frames = backtrace(crash_callstack, MAX_DEPTH);

	// Emitted before the symbols so that "the unwinder returned nothing" can be
	// told apart from "the output was truncated".
	write_literal("frames=");
	write_unsigned(static_cast<unsigned long>(num_frames > 0 ? num_frames : 0));
	write_literal("\n");

	if (num_frames > 0) {
		backtrace_symbols_fd(crash_callstack, num_frames, STDERR_FILENO);
	}
	write_literal("=== end of stack trace ===\n");

	raise(sig);
}

// Without this, an orderly shutdown looks exactly like a crash in the logs:
// silence, no banner. Reporting it means a death with neither banner is a
// SIGKILL (OOM kill or forced stop).
void log_termination(const int sig) {
	write_literal("\n=== terminated by signal ");
	write_unsigned(static_cast<unsigned long>(sig));
	write_literal(" (orderly shutdown, not a crash) ===\n");

	raise(sig);
}

// glibc loads libgcc_s.so on the first backtrace() call, which allocates and
// takes the loader lock. Doing that for the first time inside a handler can
// deadlock or fault, which produces a banner with no frames after it.
void warm_up_backtrace() {
	void* frame[1];
	(void)backtrace(frame, 1);
}

} // namespace

void Install() {
	struct sigaction crash_action = {};
	sigemptyset(&crash_action.sa_mask);
	crash_action.sa_handler = log_crash;
	// SA_RESETHAND resets the signal handler to the default. If log_crash itself segfaults, then the default handler
	// (coredump) is triggered. SA_NODEFER makes sure that if log_crash itself segfaults, the process dies immediately
	// instead of hanging until log_crash is finished.
	crash_action.sa_flags = static_cast<int>(SA_RESETHAND | SA_NODEFER);
	sigaction(SIGSEGV, &crash_action, nullptr);
	sigaction(SIGABRT, &crash_action, nullptr);
	sigaction(SIGBUS, &crash_action, nullptr);
	sigaction(SIGILL, &crash_action, nullptr);
	sigaction(SIGFPE, &crash_action, nullptr);

	struct sigaction term_action = {};
	sigemptyset(&term_action.sa_mask);
	term_action.sa_handler = log_termination;
	term_action.sa_flags = static_cast<int>(SA_RESETHAND | SA_NODEFER);
	sigaction(SIGTERM, &term_action, nullptr);
	sigaction(SIGINT, &term_action, nullptr);

	warm_up_backtrace();
}

} // namespace crash_handler
