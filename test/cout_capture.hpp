#pragma once

#include <iostream>
#include <sstream>
#include <string>

namespace test {

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

} // namespace test
