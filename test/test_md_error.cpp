#include "md_error.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cstddef>
#include <string>

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
    const std::string utf8_message =
        std::string(7990, 'A') +
        reinterpret_cast<const char *>(
            u8"😊😊😊😊😊😊😊😊😊😊"); // 'A's followed by 10 multi-byte chars
    const std::string truncated_message =
        md_error::truncate_for_grpc_header(utf8_message);
    REQUIRE(truncated_message.size() < utf8_message.size());
    const std::string suffix = "...[truncated]";
    REQUIRE(truncated_message.ends_with(suffix));
    const size_t last_content_byte_pos =
        truncated_message.size() - suffix.size() - 1;
    unsigned char last_content_byte =
        static_cast<unsigned char>(truncated_message[last_content_byte_pos]);
    // Check that last byte is not a continuation byte (10xxxxxx)
    REQUIRE((last_content_byte & 0xC0) != 0x80);
  }
}
