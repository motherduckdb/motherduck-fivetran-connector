// This is a slightly modified copy of src/common/stacktrace.cpp.
// See https://github.com/duckdb/duckdb/blob/v1.3.2/src/common/stacktrace.cpp.

#include "stacktrace.hpp"

#if defined(__GLIBC__) || defined(__APPLE__)
#include <execinfo.h>
#include <cxxabi.h>
#endif

#include <string>
#include <vector>
#include <sstream>
#include <memory>

using namespace std;

namespace duckdb_copy {

namespace StringUtil {

std::vector<std::string> Split(const std::string &str, char delimiter) {
	std::stringstream ss(str);
	std::vector<std::string> lines;
	std::string temp;
	while (getline(ss, temp, delimiter)) {
		lines.push_back(temp);
	}
	return (lines);
}

bool CharacterIsSpace(char c) {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
}

bool CharacterIsDigit(char c) {
	return c >= '0' && c <= '9';
}

bool CharacterIsHex(char c) {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

}

#if defined(__GLIBC__) || defined(__APPLE__)
static std::string UnmangleSymbol(std::string symbol) {
	// find the mangled name
	uint64_t mangle_start = symbol.size();
	uint64_t mangle_end = 0;
	for (uint64_t i = 0; i < symbol.size(); ++i) {
		if (symbol[i] == '_') {
			mangle_start = i;
			break;
		}
	}
	for (uint64_t i = mangle_start; i < symbol.size(); i++) {
		if (StringUtil::CharacterIsSpace(symbol[i]) || symbol[i] == ')' || symbol[i] == '+') {
			mangle_end = i;
			break;
		}
	}
	if (mangle_start >= mangle_end) {
		return symbol;
	}
	std::string mangled_symbol = symbol.substr(mangle_start, mangle_end - mangle_start);

	int status;
	auto demangle_result = abi::__cxa_demangle(mangled_symbol.c_str(), nullptr, nullptr, &status);
	if (status != 0 || !demangle_result) {
		return symbol;
	}
	std::string result;
	result += symbol.substr(0, mangle_start);
	result += demangle_result;
	result += symbol.substr(mangle_end);
	free(demangle_result);
	return result;
}

static std::string CleanupStackTrace(std::string symbol) {
#ifdef __APPLE__
	// structure of frame pointers is [depth] [library] [pointer] [symbol]
	// we are only interested in [depth] and [symbol]

	// find the depth
	uint64_t start;
	for (start = 0; start < symbol.size(); start++) {
		if (!StringUtil::CharacterIsDigit(symbol[start])) {
			break;
		}
	}

	// now scan forward until we find the frame pointer
	uint64_t frame_end = symbol.size();
	for (uint64_t i = start; i + 1 < symbol.size(); ++i) {
		if (symbol[i] == '0' && symbol[i + 1] == 'x') {
			uint64_t k;
			for (k = i + 2; k < symbol.size(); ++k) {
				if (!StringUtil::CharacterIsHex(symbol[k])) {
					break;
				}
			}
			frame_end = k;
			break;
		}
	}
	static constexpr uint64_t STACK_TRACE_INDENTATION = 8;
	if (frame_end == symbol.size() || start >= STACK_TRACE_INDENTATION) {
		// frame pointer not found - just preserve the original frame
		return symbol;
	}
	uint64_t space_count = STACK_TRACE_INDENTATION - start;
	return symbol.substr(0, start) + std::string(space_count, ' ') + symbol.substr(frame_end, symbol.size() - frame_end);
#else
	return symbol;
#endif
}

std::string StackTrace::GetStacktracePointers(uint64_t max_depth) {
	std::string result;
	auto callstack = unique_ptr<void *[]>(new void *[max_depth]);
	int frames = backtrace(callstack.get(), static_cast<int32_t>(max_depth));
	// skip two frames (these are always StackTrace::...)
	for (uint64_t i = 2; i < static_cast<uint64_t>(frames); i++) {
		if (!result.empty()) {
			result += ";";
		}
		result += to_string(reinterpret_cast<uintptr_t>(callstack[i]));
	}
	return result;
}

template <class SRC = uint8_t>
SRC *cast_uint64_to_pointer(uint64_t value) {
	return reinterpret_cast<SRC *>(static_cast<uintptr_t>(value));
}

std::string StackTrace::ResolveStacktraceSymbols(const std::string &pointers) {
	auto splits = StringUtil::Split(pointers, ';');
	uint64_t frame_count = splits.size();
	auto callstack = std::unique_ptr<void *[]>(new void *[frame_count]);
	for (uint64_t i = 0; i < frame_count; i++) {
		callstack[i] = cast_uint64_to_pointer(std::stoull(splits[i]));
	}
	std::string result;
	char **strs = backtrace_symbols(callstack.get(), static_cast<int32_t>(frame_count));
	for (uint64_t i = 0; i < frame_count; i++) {
		result += CleanupStackTrace(UnmangleSymbol(strs[i]));
		result += "\n";
	}
	free(reinterpret_cast<void *>(strs));
	return "\n" + result;
}

#else
std::string StackTrace::GetStacktracePointers(uint64_t max_depth) {
	return std::string();
}

std::string StackTrace::ResolveStacktraceSymbols(const std::string &pointers) {
	return std::string();
}
#endif

} // namespace duckdb_copy
