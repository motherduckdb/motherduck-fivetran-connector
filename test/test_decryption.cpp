#include "decryption.hpp"
#include "openssl_helper.hpp"

#include <cassert>
#include <catch2/catch_all.hpp>
#include <cerrno>
#include <filesystem>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <vector>

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
  const auto init_result = EVP_EncryptInit_ex2(
      ctx, aes_impl, reinterpret_cast<const unsigned char *>(key.c_str()),
      reinterpret_cast<const unsigned char *>(iv.c_str()), nullptr);
  // Decrease reference count of aes_impl; EVP_EncryptInit_ex2 has incremented
  // the reference count
  EVP_CIPHER_free(aes_impl);
  if (init_result != 1) {
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
  std::vector<unsigned char> input_buffer(buffer_size);
  // "For most ciphers and modes, the amount of data written can be anything
  // from zero bytes to (inl + cipher_block_size - 1) bytes."
  std::vector<unsigned char> ciphertext_buffer(buffer_size + aes_block_size);
  int ciphertext_length = 0;

  // The istream bool operator evaluates to true if all requested bytes could be
  // read and no error occurred. The gcount() function returns the number of
  // bytes read by the last read operation.
  while (input.read(reinterpret_cast<char *>(input_buffer.data()), buffer_size) ||
         input.gcount() > 0) {
    // Stream is only allowed to fail if EOF has been reached
    if (input.bad() || (input.fail() && !input.eof())) {
      throw std::system_error(errno, std::iostream_category(),
                              "Error when reading input stream");
    }

    const auto bytes_read = static_cast<int>(input.gcount());
    if (1 != EVP_EncryptUpdate(ctx, ciphertext_buffer.data(), &ciphertext_length,
                               input_buffer.data(), bytes_read)) {
      openssl_helper::raise_openssl_error("Error during encryption update");
    }
    output.write(reinterpret_cast<char *>(ciphertext_buffer.data()),
                 ciphertext_length);
  }

  if (!EVP_EncryptFinal_ex(ctx, ciphertext_buffer.data(), &ciphertext_length)) {
    openssl_helper::raise_openssl_error("Error during encryption finalization");
  }
  output.write(reinterpret_cast<char *>(ciphertext_buffer.data()), ciphertext_length);
}
} // namespace

TEST_CASE("Decrypt is inverse function of encrypt") {
  // 32 bytes for AES-256 key
  std::string key = generate_random_string(32);

  std::string plaintext = generate_random_string(100000000);
  std::istringstream plaintext_stream(plaintext);

  std::stringstream ciphertext_stream;
  encrypt_stream(plaintext_stream, ciphertext_stream, key);

  auto result =
      decrypt_stream(ciphertext_stream, "<memory stream>",
                     reinterpret_cast<const unsigned char *>(key.c_str()));
  std::string result_str(result.begin(), result.end());

  REQUIRE(result_str == plaintext);
}