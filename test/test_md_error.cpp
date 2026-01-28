#include "md_error.hpp"

#include "catch2/generators/catch_generators.hpp"
#include <catch2/catch_test_macros.hpp>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include <iostream>

namespace {
bool has_valid_utf8_end(const std::string_view str) {
  if (str.empty()) {
    return true;
  }

  const auto last_byte_pos = str.size() - 1;
  const unsigned char last_byte =
      static_cast<unsigned char>(str[last_byte_pos]);
  // If the last byte is an ASCII character (0xxxxxxx), the string is valid.
  if ((last_byte & 0b10000000) == 0) {
    return true;
  }
  // If the last byte is the start of a multi-byte character (11xxxxxx), the
  // string is invalid.
  if ((last_byte & 0b11000000) == 0b11000000) {
    return false;
  }

  // If the last byte is a continuation byte (10xxxxxx), we need to look back to
  // find the start of the multi-byte character and count how many continuation
  // bytes there are (1, 2 or 3).
  std::uint8_t num_continuation_bytes = 0;
  auto start_byte_pos = last_byte_pos;
  while (start_byte_pos > 0 &&
         (static_cast<unsigned char>(str[start_byte_pos]) & 0b11000000) ==
             0b10000000) {
    ++num_continuation_bytes;
    --start_byte_pos;
  }

  // We then check that the start byte is either 110xxxxx (2-byte character),
  // 1110xxxx (3-byte character) or 11110xxx (4-byte character).
  const unsigned char start_byte =
      static_cast<unsigned char>(str[start_byte_pos]);
  if (num_continuation_bytes == 1) {
    return (start_byte & 0b11100000) == 0b11000000;
  } else if (num_continuation_bytes == 2) {
    return (start_byte & 0b11110000) == 0b11100000;
  } else if (num_continuation_bytes == 3) {
    return (start_byte & 0b11111000) == 0b11110000;
  }

  return false;
}
} // namespace

TEST_CASE("Test truncate_for_grpc_header", "[md_error]") {
  SECTION("Check that short messages are not truncated") {
    const std::string short_message = "This is a short error message.";
    REQUIRE(md_error::truncate_for_grpc_header(short_message) == short_message);
  }

  SECTION("Check that too long messages are truncated") {
    const std::string long_message(9000, 'A');
    const std::string truncated_message =
        md_error::truncate_for_grpc_header(long_message);
    REQUIRE(truncated_message.size() < long_message.size());
    REQUIRE(truncated_message.ends_with("...[truncated]"));
  }

  SECTION(
      "Check that UTF-8 messages are truncated without breaking characters") {
    const unsigned int offset = GENERATE(0u, 1u, 2u, 3u);
    std::string utf8_message(7950 + offset, 'A');
    for (std::uint8_t i = 0; i < 100; ++i) {
      // Multi-byte characters
      utf8_message += reinterpret_cast<const char *>(u8"😊");
    }

    const std::string truncated_message =
        md_error::truncate_for_grpc_header(utf8_message);
    REQUIRE(truncated_message.size() < utf8_message.size());
    const std::string suffix = "...[truncated]";
    REQUIRE(truncated_message.ends_with(suffix));

    const std::string_view message_content(
        truncated_message.data(), truncated_message.size() - suffix.size());
    REQUIRE(has_valid_utf8_end(message_content));
  }
}
