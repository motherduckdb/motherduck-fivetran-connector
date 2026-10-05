#include "md_logging.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using Catch::Matchers::ContainsSubstring;

namespace {

// Redirects std::cout into a buffer for the lifetime of the object.
class CoutCapture {
public:
	CoutCapture() : original_buffer(std::cout.rdbuf(buffer.rdbuf())) {
	}
	~CoutCapture() {
		std::cout.rdbuf(original_buffer);
	}
	CoutCapture(const CoutCapture&) = delete;
	CoutCapture& operator=(const CoutCapture&) = delete;

	std::string str() const {
		return buffer.str();
	}

private:
	std::stringstream buffer;
	std::streambuf* original_buffer;
};

std::vector<std::string> split_lines(const std::string& output) {
	std::vector<std::string> lines;
	std::istringstream stream(output);
	for (std::string line; std::getline(stream, line);) {
		lines.push_back(line);
	}
	return lines;
}

} // namespace

TEST_CASE("Log records are escaped as JSON strings", "[md_logging]") {
	const auto logger = mdlog::Logger::CreateStdoutLogger();

	SECTION("Quotes and backslashes are backslash-escaped") {
		CoutCapture capture;
		logger.info("path \"C:\\tmp\" missing");

		REQUIRE_THAT(capture.str(), ContainsSubstring("\"message\":\"path \\\"C:\\\\tmp\\\" missing,"));
	}

	SECTION("A multi-line message stays on one line") {
		CoutCapture capture;
		logger.severe("Parser Error:\nsyntax error\r\n\tat line 1");

		const auto lines = split_lines(capture.str());
		REQUIRE(lines.size() == 1);
		REQUIRE_THAT(lines[0], ContainsSubstring("Parser Error:\\nsyntax error\\r\\n\\tat line 1"));
	}

	SECTION("Backspace and form feed get their named escapes") {
		CoutCapture capture;
		logger.info("back\bfeed\fend");

		REQUIRE_THAT(capture.str(), ContainsSubstring("back\\bfeed\\fend"));
	}

	SECTION("Other control characters become \\u escapes") {
		CoutCapture capture;
		// Split literal: "\x07e" would otherwise be read as one hex escape.
		logger.info("bell\x07"
		            "end");

		REQUIRE_THAT(capture.str(), ContainsSubstring("bell\\u0007end"));
	}
}

TEST_CASE("Concurrent logging emits one intact record per line", "[md_logging]") {
	const auto logger = mdlog::Logger::CreateStdoutLogger();

	constexpr unsigned int num_threads = 8;
	constexpr unsigned int records_per_thread = 250;

	std::string output;
	{
		CoutCapture capture;
		std::vector<std::thread> threads;
		for (unsigned int t = 0; t < num_threads; t++) {
			threads.emplace_back([&logger, t]() {
				for (unsigned int i = 0; i < records_per_thread; i++) {
					logger.info("record thread=" + std::to_string(t) + " seq=" + std::to_string(i));
				}
			});
		}
		for (auto& thread : threads) {
			thread.join();
		}
		output = capture.str();
	}

	const auto lines = split_lines(output);
	REQUIRE(lines.size() == num_threads * records_per_thread);

	// Every record must be whole: a spliced line has a second record's prefix somewhere in its middle.
	std::set<std::string> seen_messages;
	for (const auto& line : lines) {
		const std::string prefix = "{\"level\":\"INFO\",\"message\":\"record thread=";
		const std::string suffix = "\",\"message-origin\":\"sdk_destination\"}";
		REQUIRE(line.rfind(prefix, 0) == 0);
		REQUIRE(line.size() > suffix.size());
		REQUIRE(line.compare(line.size() - suffix.size(), suffix.size(), suffix) == 0);
		REQUIRE(line.find(prefix, 1) == std::string::npos);
		seen_messages.insert(line);
	}
	REQUIRE(seen_messages.size() == num_threads * records_per_thread);
}
