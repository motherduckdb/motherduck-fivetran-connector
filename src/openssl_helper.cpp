#include "openssl_helper.hpp"

#include <openssl/err.h>
#include <sstream>
#include <string>

/// Raises a std::runtime_error containing all OpenSSL error message in the
/// queue, prefixed by message_prefix. message_prefix should not end with a
/// punctuation mark. The OpenSSL error queue is thread-local, so this function
/// retrieves errors for the current thread only.
void openssl_helper::raise_openssl_error(const std::string& message_prefix) {
	std::ostringstream final_error_stream;
	final_error_stream << message_prefix;

	unsigned long error_code;
	unsigned int error_num = 0;
	char error_string_buf[256];

	while ((error_code = ERR_get_error()) != 0) {
		if (error_num == 0) {
			final_error_stream << ". OpenSSL error:";
		}
		error_num++;

		// ERR_error_string_n() null-terminates the string, so we don't have to null
		// error_string_buf
		ERR_error_string_n(error_code, error_string_buf, sizeof(error_string_buf));
		final_error_stream << '\n' << error_string_buf;
	}

	throw std::runtime_error(final_error_stream.str());
}
