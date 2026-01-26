#include "decryption.hpp"
#include "openssl_helper.hpp"

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/generators/catch_generators_adapters.hpp>
#include <catch2/generators/catch_generators_random.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <vector>

#include "memory_backed_file.hpp"

#define STRING(x) #x
#define XSTRING(s) STRING(s)
const std::string TEST_RESOURCES_DIR = XSTRING(TEST_RESOURCES_LOCATION);

namespace fs = std::filesystem;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#pragma GCC diagnostic ignored "-Wshorten-64-to-32"

namespace {
std::vector<std::byte> generate_random_bytes(const size_t length) {
  std::vector<std::byte> result;
  result.resize(length);

  if (length > static_cast<size_t>(std::numeric_limits<int>::max())) {
    throw std::overflow_error(
        "Requested length for random bytes exceeds maximum int value");
  }

  if (RAND_bytes(reinterpret_cast<unsigned char *>(&result[0]),
                 static_cast<int>(length)) != 1) {
    throw std::runtime_error("Failed to generate random bytes");
  }

  return result;
}

std::istringstream generate_random_string_stream(const size_t length) {
  const std::vector<std::byte> random_bytes = generate_random_bytes(length);
  return std::istringstream(
      std::string(reinterpret_cast<const char *>(random_bytes.data()),
                  random_bytes.size()));
}

void encrypt_stream(std::istream &input, std::ostream &output,
                    const std::vector<std::byte> &encryption_key) {
  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (ctx == nullptr) {
    openssl_helper::raise_openssl_error(
        "Failed to create encryption cipher context");
  }
  openssl_helper::CipherCtxDeleter ctx_deleter(ctx);

  constexpr const char *algorithm = "AES-256-CBC"; // Defined in EVP_CIPHER-AES
  EVP_CIPHER *aes_impl =
      EVP_CIPHER_fetch(nullptr /* default library context */, algorithm,
                       "provider=default" /* properties */);
  if (aes_impl == nullptr) {
    openssl_helper::raise_openssl_error(
        "Failed to fetch encryption cipher implementation for " +
        std::string(algorithm));
  }

  const int aes_key_length = EVP_CIPHER_get_key_length(aes_impl);
  if (encryption_key.size() != static_cast<size_t>(aes_key_length)) {
    EVP_CIPHER_free(aes_impl);
    throw std::runtime_error("Keys for algorithm " + std::string(algorithm) +
                             " must be " + std::to_string(aes_key_length) +
                             " bytes long, but the provided key has length " +
                             std::to_string(encryption_key.size()));
  }

  const int aes_block_size = EVP_CIPHER_get_block_size(aes_impl);

  const int iv_length = EVP_CIPHER_get_iv_length(aes_impl);
  assert(iv_length == aes_block_size);
  const std::vector<std::byte> iv = generate_random_bytes(iv_length);
  assert(iv.size() == static_cast<size_t>(iv_length));

  // Sets up cipher context ctx for encryption with cipher type aes_impl.
  // PKCS padding is enabled by default.
  const int init_result = EVP_EncryptInit_ex2(
      ctx, aes_impl,
      reinterpret_cast<const unsigned char *>(encryption_key.data()),
      reinterpret_cast<const unsigned char *>(iv.data()), nullptr);
  // Decrease reference count of aes_impl; EVP_EncryptInit_ex2 has incremented
  // the reference count
  EVP_CIPHER_free(aes_impl);
  if (1 != init_result) {
    openssl_helper::raise_openssl_error(
        "Failed to initialize encryption cipher context for " +
        std::string(algorithm));
  }

  // First, write IV to the output stream
  output.write(reinterpret_cast<const char *>(iv.data()), iv_length);

  // If we want to use in-place encryption, consider this:
  // "However, in-place encryption is guaranteed to work only if the encryption
  // context (ctx) has processed data in multiples of the block size."
  const int buffer_size = aes_block_size * 1024;
  std::vector<std::byte> input_buffer;
  input_buffer.resize(buffer_size);
  // "For most ciphers and modes, the amount of data written can be anything
  // from zero bytes to (inl + cipher_block_size - 1) bytes."
  std::vector<std::byte> ciphertext_buffer;
  ciphertext_buffer.resize(buffer_size + aes_block_size);
  int ciphertext_length = 0;

  // The istream bool operator evaluates to true if all requested bytes could be
  // read and no error occurred. The gcount() function returns the number of
  // bytes read by the last read operation.
  while (
      input.read(reinterpret_cast<char *>(input_buffer.data()), buffer_size) ||
      input.gcount() > 0) {
    // Stream is only allowed to fail if EOF has been reached
    if (input.fail() && !input.eof()) {
      throw std::system_error(errno, std::iostream_category(),
                              "Error when reading input stream");
    }

    const auto bytes_read = static_cast<int>(input.gcount());
    if (1 != EVP_EncryptUpdate(
                 ctx,
                 reinterpret_cast<unsigned char *>(ciphertext_buffer.data()),
                 &ciphertext_length,
                 reinterpret_cast<const unsigned char *>(input_buffer.data()),
                 bytes_read)) {
      openssl_helper::raise_openssl_error("Error during encryption update");
    }
    output.write(reinterpret_cast<const char *>(ciphertext_buffer.data()),
                 ciphertext_length);
  }

  if (1 != EVP_EncryptFinal_ex(
               ctx, reinterpret_cast<unsigned char *>(ciphertext_buffer.data()),
               &ciphertext_length)) {
    openssl_helper::raise_openssl_error("Error during encryption finalization");
  }
  output.write(reinterpret_cast<const char *>(ciphertext_buffer.data()),
               ciphertext_length);
}
} // namespace

TEST_CASE("Decrypt is inverse function of encrypt", "[decryption]") {
  // 32 bytes for AES-256 key
  std::vector<std::byte> key = generate_random_bytes(32);

  const auto random_factor = GENERATE(take(100, random(1.0, 100.0)));
  const auto plaintext_length = static_cast<size_t>(1000000 * random_factor);
  std::istringstream plaintext_stream =
      generate_random_string_stream(plaintext_length);

  std::stringstream ciphertext;
  encrypt_stream(plaintext_stream, ciphertext, key);

  std::ostringstream result;
  decryption::decrypt_stream(ciphertext, "<memory stream>", result, key);

  plaintext_stream.clear();
  plaintext_stream.seekg(0);
  REQUIRE(result.str() == plaintext_stream.str());
}

TEST_CASE("Decrypt file that was encrypted using openssl util produces "
          "original plaintext",
          "[decryption]") {
  std::vector<std::byte> key;
  {
    const auto key_path = fs::path(TEST_RESOURCES_DIR) / "encrypted" /
                          "customers-100000.csv.aes.key";
    constexpr int key_size = 32;
    key.resize(32);
    std::ifstream key_file(key_path, std::ios::binary);
    REQUIRE((key_file && key_file.is_open()));
    key_file.read(reinterpret_cast<char *>(key.data()), key_size);
  }

  std::ostringstream decrypted_text;
  {
    const auto encrypted_file_path =
        fs::path(TEST_RESOURCES_DIR) / "encrypted" / "customers-100000.csv.aes";
    decryption::decrypt_file(encrypted_file_path.string(), decrypted_text, key);
  }

  std::string plaintext;
  {
    const auto plaintext_file_path =
        fs::path(TEST_RESOURCES_DIR) / "encrypted" / "customers-100000.csv";
    std::ifstream plaintext_file(plaintext_file_path);
    REQUIRE((plaintext_file && plaintext_file.is_open()));
    plaintext = std::string(std::istreambuf_iterator<char>(plaintext_file),
                            std::istreambuf_iterator<char>());
  }

  REQUIRE(decrypted_text.str() == plaintext);
}

TEST_CASE("Decryption functions throw correct errors", "[decryption]") {
  SECTION("decrypt_file throws if file does not exist") {
    std::ostringstream output;
    std::string key = "somekey";
    std::vector<std::byte> key_bytes(
        reinterpret_cast<const std::byte *>(key.data()),
        reinterpret_cast<const std::byte *>(key.data() + key.size()));
    REQUIRE_THROWS_WITH(
        decryption::decrypt_file("non_existent_file.csv.enc", output,
                                 key_bytes),
        Catch::Matchers::ContainsSubstring("No such file or directory"));
  }

  SECTION("decrypt_stream throws on invalid key") {
    std::istringstream input;
    std::ostringstream output;
    std::string key = "too_short_key";
    std::vector<std::byte> key_bytes(
        reinterpret_cast<const std::byte *>(key.data()),
        reinterpret_cast<const std::byte *>(key.data() + key.size()));

    REQUIRE_THROWS_WITH(
        decryption::decrypt_stream(input, "<memory stream>", output, key_bytes),
        Catch::Matchers::ContainsSubstring(
            "Decryption key must be 32 bytes long for AES-256-CBC"));
  }

  SECTION("decrypt_stream throws if file is empty") {
    std::stringstream input;
    std::ostringstream output;
    std::vector<std::byte> key = generate_random_bytes(32);

    REQUIRE_THROWS_WITH(
        decryption::decrypt_stream(input, "<memory stream>", output, key),
        Catch::Matchers::ContainsSubstring(
            "Unexpected end of file while reading IV"));
  }

  SECTION("decrypt_stream throws if file is too short") {
    std::stringstream input;
    input << "too_little_data";
    std::ostringstream output;
    std::vector<std::byte> key = generate_random_bytes(32);

    REQUIRE_THROWS_WITH(
        decryption::decrypt_stream(input, "<memory stream>", output, key),
        Catch::Matchers::ContainsSubstring(
            "Unexpected end of file while reading IV"));
  }

  SECTION("decrypt_stream throws if input stream contains garbage") {
    std::stringstream input;
    // Make input long enough to successfully read IV
    input << "1111111111111111_garbage_data";
    std::ostringstream output;
    std::vector<std::byte> key = generate_random_bytes(32);

    REQUIRE_THROWS_WITH(
        decryption::decrypt_stream(input, "<memory stream>", output, key),
        Catch::Matchers::ContainsSubstring(
            "Error during decrypt finalization"));
  }

  SECTION("decrypt_stream produces garbage if input contains garbage in the "
          "middle of the stream") {
    std::vector<std::byte> key = generate_random_bytes(32);
    // Make string bigger than one buffer size (256 KiB).
    std::istringstream plaintext_stream =
        generate_random_string_stream(256 * 1024 + 10000);

    std::stringstream ciphertext_stream;
    encrypt_stream(plaintext_stream, ciphertext_stream, key);
    std::string ciphertext = ciphertext_stream.str();
    // Corrupt data, but don't change length of ciphertext
    ciphertext.replace(256 * 1024 + 1000, 10, "garbage!!!");
    ciphertext_stream.str(ciphertext);

    std::ostringstream output;
    // Decryption still succeeds because ciphertext length was not changed
    decryption::decrypt_stream(ciphertext_stream, "<memory stream>", output,
                               key);
    plaintext_stream.clear();
    plaintext_stream.seekg(0);
    REQUIRE(output.str() != plaintext_stream.str());
  }

  SECTION("decrypt_stream throws if input is too short") {
    std::vector<std::byte> key = generate_random_bytes(32);
    // Make string bigger than one buffer size (256 KiB).
    std::istringstream plaintext_stream =
        generate_random_string_stream(256 * 1024 + 10000);

    std::stringstream ciphertext_stream;
    encrypt_stream(plaintext_stream, ciphertext_stream, key);
    std::string ciphertext = ciphertext_stream.str();
    // Remove a few characters after the first iteration (after 64 KiB).
    ciphertext.replace(90000, 10, "");
    ciphertext_stream.str(ciphertext);

    std::ostringstream output;
    REQUIRE_THROWS_WITH(
        decryption::decrypt_stream(ciphertext_stream, "<memory stream>", output,
                                   key),
        Catch::Matchers::ContainsSubstring("wrong final block length"));
  }

  SECTION("decrypt_stream throws if it cannot write to the output stream") {
    std::vector<std::byte> key = generate_random_bytes(32);
    std::istringstream plaintext_stream = generate_random_string_stream(100);

    std::stringstream ciphertext_stream;
    encrypt_stream(plaintext_stream, ciphertext_stream, key);

    std::ostringstream output;
    // Make writes fail
    output.setstate(std::ios::failbit);

    REQUIRE_THROWS_WITH(
        decryption::decrypt_stream(ciphertext_stream, "<memory stream>", output,
                                   key),
        Catch::Matchers::ContainsSubstring("Error writing to output stream"));
  }
}

#pragma GCC diagnostic pop
