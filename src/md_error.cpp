#include "md_error.hpp"

#include <string>

namespace md_error {
std::string truncate_for_grpc_header(const std::string &message) {
  constexpr std::string_view TRUNCATION_SUFFIX = "...[truncated]";
  constexpr std::size_t TRUNCATION_SUFFIX_LENGTH = TRUNCATION_SUFFIX.size();
  // Maximum header size on Fivetran platform is currently 10240 bytes.
  // We truncate the error message after 8000 bytes to leave enough space.
  constexpr size_t GRPC_ERROR_MAX_LENGTH = 8000;
  constexpr size_t TARGET_LEN =
      GRPC_ERROR_MAX_LENGTH - TRUNCATION_SUFFIX_LENGTH;

  if (message.size() <= GRPC_ERROR_MAX_LENGTH) {
    return message;
  }

  // Find UTF-8 safe truncation point by walking backward from target position.
  // See https://stackoverflow.com/a/35328573/8336143
  size_t pos = TARGET_LEN;
  while (pos > 0 && (static_cast<unsigned char>(message[pos]) & 0xC0) == 0x80) {
    --pos; // Skip continuation bytes (10xxxxxx)
  }

  return message.substr(0, pos) + std::string(TRUNCATION_SUFFIX);
}
} // namespace md_error
