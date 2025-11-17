#include "decryption.hpp"
#include "openssl_helper.hpp"

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <sstream>
#include <stdexcept>
#include <system_error>

#define STRING(x) #x
#define XSTRING(s) STRING(s)
const std::string TEST_RESOURCES_DIR = XSTRING(TEST_RESOURCES_LOCATION);

namespace fs = std::filesystem;

namespace {
std::string generate_random_string(const size_t length) {
  std::string result(length, '\0');

  if (RAND_bytes(reinterpret_cast<unsigned char *>(&result[0]), length) != 1) {
    throw std::runtime_error("Failed to generate random bytes");
  }

  return result;
}

void encrypt_stream(std::istream &input, std::ostream &output,
                    const std::string &key) {
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
  if (key.size() != static_cast<size_t>(aes_key_length)) {
    EVP_CIPHER_free(aes_impl);
    throw std::runtime_error("Keys for algorithm " + std::string(algorithm) +
                             " must be " + std::to_string(aes_key_length) +
                             " bytes long, but the provided key has length " +
                             std::to_string(key.size()));
  }

  const int aes_block_size = EVP_CIPHER_get_block_size(aes_impl);

  const int iv_length = EVP_CIPHER_get_iv_length(aes_impl);
  assert(iv_length == aes_block_size);
  const std::string iv = generate_random_string(iv_length);
  assert(iv.size() == static_cast<size_t>(iv_length));

  // Sets up cipher context ctx for encryption with cipher type aes_impl.
  // PKCS padding is enabled by default.
  const int init_result = EVP_EncryptInit_ex2(
      ctx, aes_impl, reinterpret_cast<const unsigned char *>(key.data()),
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
  output.write(iv.c_str(), iv_length);

  // If we want to use in-place encryption, consider this:
  // "However, in-place encryption is guaranteed to work only if the encryption
  // context (ctx) has processed data in multiples of the block size."
  const int buffer_size = aes_block_size * 512;
  unsigned char input_buffer[buffer_size];
  // "For most ciphers and modes, the amount of data written can be anything
  // from zero bytes to (inl + cipher_block_size - 1) bytes."
  unsigned char ciphertext_buffer[buffer_size + aes_block_size];
  int ciphertext_length = 0;

  // The istream bool operator evaluates to true if all requested bytes could be
  // read and no error occurred. The gcount() function returns the number of
  // bytes read by the last read operation.
  while (input.read(reinterpret_cast<char *>(input_buffer), buffer_size) ||
         input.gcount() > 0) {
    // Stream is only allowed to fail if EOF has been reached
    if (input.fail() && !input.eof()) {
      throw std::system_error(errno, std::iostream_category(),
                              "Error when reading input stream");
    }

    const auto bytes_read = static_cast<int>(input.gcount());
    if (1 != EVP_EncryptUpdate(ctx, ciphertext_buffer, &ciphertext_length,
                               input_buffer, bytes_read)) {
      openssl_helper::raise_openssl_error("Error during encryption update");
    }
    output.write(reinterpret_cast<char *>(ciphertext_buffer),
                 ciphertext_length);
  }

  if (1 != EVP_EncryptFinal_ex(ctx, ciphertext_buffer, &ciphertext_length)) {
    openssl_helper::raise_openssl_error("Error during encryption finalization");
  }
  output.write(reinterpret_cast<char *>(ciphertext_buffer), ciphertext_length);
}
} // namespace

TEST_CASE("Decrypt is inverse function of encrypt", "[decryption]") {
  // 32 bytes for AES-256 key
  std::string key = generate_random_string(32);

  std::string plaintext = generate_random_string(100000000);
  std::istringstream plaintext_stream(plaintext);

  std::stringstream ciphertext_stream;
  encrypt_stream(plaintext_stream, ciphertext_stream, key);

  std::ostringstream result;
  decrypt_stream(ciphertext_stream, "<memory stream>", result, key);

  REQUIRE(result.str() == plaintext);
}

TEST_CASE("Decrypt file that was encrypted using openssl util produces "
          "original plaintext",
          "[decryption]") {

  std::string key, decrypted_text, plaintext;

  {
    const auto key_path =
        fs::path(TEST_RESOURCES_DIR) / "encrypted" / "customers-100000.key";
    std::ifstream key_file(key_path, std::ios::binary);
    REQUIRE((key_file && key_file.is_open()));
    std::getline(key_file, key);
  }

  {
    const auto encrypted_file_path =
        fs::path(TEST_RESOURCES_DIR) / "encrypted" / "customers-100000.csv.enc";
    decrypted_text = decrypt_file(encrypted_file_path.string(), key);
  }

  {
    const auto plaintext_file_path =
        fs::path(TEST_RESOURCES_DIR) / "encrypted" / "customers-100000.csv";
    std::ifstream plaintext_file(plaintext_file_path);
    REQUIRE((plaintext_file && plaintext_file.is_open()));
    plaintext = std::string((std::istreambuf_iterator<char>(plaintext_file)),
                            std::istreambuf_iterator<char>());
  }

  REQUIRE(decrypted_text == plaintext);
}

TEST_CASE("Decryption functions throw correct errors", "[decryption]") {
  SECTION("decrypt_file throws if file does not exist") {
    REQUIRE_THROWS_WITH(
        decrypt_file("non_existent_file.csv.enc", "somekey"),
        Catch::Matchers::ContainsSubstring("No such file or directory"));
  }

  SECTION("decrypt_stream throws on invalid key") {
    std::istringstream input;
    std::ostringstream output;

    REQUIRE_THROWS_WITH(
        decrypt_stream(input, "<memory stream>", output, "too_short_key"),
        Catch::Matchers::ContainsSubstring(
            "Decryption key must be 32 bytes long for AES-256-CBC"));
  }

  SECTION("decrypt_stream throws if file is empty") {
    std::stringstream input;
    std::ostringstream output;
    std::string key = generate_random_string(32);

    REQUIRE_THROWS_WITH(decrypt_stream(input, "<memory stream>", output, key),
                        Catch::Matchers::ContainsSubstring(
                            "Unexpected end of file while reading IV"));
  }

  SECTION("decrypt_stream throws if file is too short") {
    std::stringstream input;
    input << "too_little_data";
    std::ostringstream output;
    std::string key = generate_random_string(32);

    REQUIRE_THROWS_WITH(decrypt_stream(input, "<memory stream>", output, key),
                        Catch::Matchers::ContainsSubstring(
                            "Unexpected end of file while reading IV"));
  }

  SECTION("decrypt_stream throws if input stream contains garbage") {
    std::stringstream input;
    // Make input long enough to successfully read IV
    input << "1111111111111111_garbage_data";
    std::ostringstream output;
    std::string key = generate_random_string(32);

    REQUIRE_THROWS_WITH(decrypt_stream(input, "<memory stream>", output, key),
                        Catch::Matchers::ContainsSubstring(
                            "Error during decrypt finalization"));
  }

  SECTION("decrypt_stream throws if input contains garbage in the middle of "
          "the stream") {
    std::string key = generate_random_string(32);
    // Make string bigger than one buffer size (64 KiB).
    std::string plaintext = generate_random_string(1000);
    std::istringstream plaintext_stream(plaintext);

    std::stringstream ciphertext_stream;
    encrypt_stream(plaintext_stream, ciphertext_stream, key);
    std::string ciphertext = ciphertext_stream.str();
    // Corrupt data, but don't change length of ciphertext
    ciphertext.replace(500, 10, "garbage!!!");
    ciphertext_stream.str(ciphertext);

    std::ostringstream output;
    // Decryption still succeeds because ciphertext length was not changed
    decrypt_stream(ciphertext_stream, "<memory stream>", output, key);
    REQUIRE(output.str() != plaintext);
  }

  SECTION("decrypt_stream throws if input is too short") {
    std::string key = generate_random_string(32);
    // Make string bigger than one buffer size (64 KiB).
    std::string plaintext = generate_random_string(100000);
    std::istringstream plaintext_stream(plaintext);

    std::stringstream ciphertext_stream;
    encrypt_stream(plaintext_stream, ciphertext_stream, key);
    std::string ciphertext = ciphertext_stream.str();
    // Remove a few characters after the first iteration (after 64 KiB).
    ciphertext.replace(90000, 10, "");
    ciphertext_stream.str(ciphertext);

    std::ostringstream output;
    REQUIRE_THROWS_WITH(
        decrypt_stream(ciphertext_stream, "<memory stream>", output, key),
        Catch::Matchers::ContainsSubstring("wrong final block length"));
  }

  SECTION("decrypt_stream throws if it cannot write to the output stream") {
    std::string key = generate_random_string(32);
    std::string plaintext = generate_random_string(100);
    std::istringstream plaintext_stream(plaintext);

    std::stringstream ciphertext_stream;
    encrypt_stream(plaintext_stream, ciphertext_stream, key);

    std::ostringstream output;
    // Make writes fail
    output.setstate(std::ios::failbit);

    REQUIRE_THROWS_WITH(
        decrypt_stream(ciphertext_stream, "<memory stream>", output, key),
        Catch::Matchers::ContainsSubstring("Error writing to output stream"));
  }
}