#include "decryption.hpp"
#include "openssl_helper.hpp"

#include <cassert>
#include <cstddef>
#include <fstream>
#include <iostream>
#include <istream>
#include <openssl/evp.h>
#include <ostream>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

namespace decryption {
namespace {
std::vector<std::byte> read_iv(std::istream &input,
                               const std::string &input_name) {
  constexpr int iv_length = 16;
  std::vector<std::byte> iv(iv_length);

  input.read(reinterpret_cast<char *>(iv.data()), iv_length);
  const std::streamsize bytes_read = input.gcount();

  if (input.fail()) {
    if (input.eof()) {
      throw std::runtime_error(
          "Unexpected end of file while reading IV from file " + input_name +
          ". Read " + std::to_string(bytes_read) + " bytes");
    } else {
      throw std::system_error(errno, std::iostream_category(),
                              "Error reading IV from file " + input_name);
    }
  }

  assert(bytes_read == iv_length);
  return iv;
}

void initialize_cipher_context(
    EVP_CIPHER_CTX *ctx, const std::string &input_name,
    const std::span<const std::byte> iv,
    const std::span<const std::byte> decryption_key) {
  constexpr const char *algorithm = "AES-256-CBC"; // Defined in EVP_CIPHER-AES
  EVP_CIPHER *aes_impl =
      EVP_CIPHER_fetch(nullptr /* default library context */, algorithm,
                       "provider=default" /* properties */);
  if (aes_impl == nullptr) {
    openssl_helper::raise_openssl_error(
        "Failed to fetch decryption cipher implementation for " +
        std::string(algorithm));
  }

  const int init_result = EVP_DecryptInit_ex2(
      ctx, aes_impl,
      reinterpret_cast<const unsigned char *>(decryption_key.data()),
      reinterpret_cast<const unsigned char *>(iv.data()), nullptr);
  // EVP_CIPHER is ref-counted and the counter got increased by
  // EVP_DecryptInit_ex2. We can let go of our own reference.
  EVP_CIPHER_free(aes_impl);
  if (1 != init_result) {
    openssl_helper::raise_openssl_error(
        "Failed to initialize decryption context for file " + input_name);
  }
}

void decrypt_stream_content(std::istream &input, const std::string &input_name,
                            std::ostream &output, EVP_CIPHER_CTX *ctx) {
  constexpr int buffer_size = 256 * 1024; // 256 KiB
  std::vector<std::byte> ciphertext_buffer;
  ciphertext_buffer.resize(buffer_size);
  // "if padding is enabled the decrypted data buffer out passed to
  // EVP_DecryptUpdate() should have sufficient room for (inl +
  // cipher_block_size) bytes"
  std::vector<std::byte> plaintext_buffer;
  plaintext_buffer.resize(buffer_size + AES_BLOCK_SIZE);
  int plaintext_length;

  // Read 256 KiB from the stream into the input buffer, decrypt it, and write
  // the plaintext to the output stream.

  // The istream bool operator evaluates to true if all requested bytes could be
  // read and no error occurred. The gcount() function returns the number of
  // bytes read by the last read operation.
  while (input.read(reinterpret_cast<char *>(ciphertext_buffer.data()),
                    buffer_size) ||
         input.gcount() > 0) {
    if (input.fail() && !input.eof()) {
      throw std::system_error(errno, std::iostream_category(),
                              "Error when reading input stream");
    }

    const auto bytes_read = static_cast<int>(input.gcount());
    if (1 !=
        EVP_DecryptUpdate(
            ctx, reinterpret_cast<unsigned char *>(plaintext_buffer.data()),
            &plaintext_length,
            reinterpret_cast<const unsigned char *>(ciphertext_buffer.data()),
            bytes_read)) {
      openssl_helper::raise_openssl_error(
          "Error during decrypt update of file " + input_name);
    }

    assert(plaintext_length <= bytes_read + AES_BLOCK_SIZE);
    output.write(reinterpret_cast<const char *>(plaintext_buffer.data()),
                 plaintext_length);
    if (!output) {
      throw std::system_error(
          errno, std::iostream_category(),
          "Error writing to output stream after decrypt update of file " +
              input_name);
    }
  }

  if (1 != EVP_DecryptFinal_ex(
               ctx, reinterpret_cast<unsigned char *>(plaintext_buffer.data()),
               &plaintext_length)) {
    openssl_helper::raise_openssl_error(
        "Error during decrypt finalization of file " + input_name);
  }

  output.write(reinterpret_cast<const char *>(plaintext_buffer.data()),
               plaintext_length);
  if (!output) {
    throw std::system_error(
        errno, std::iostream_category(),
        "Error writing to output stream after decrypt finalization of file " +
            input_name);
  }
}
} // namespace

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"

void decrypt_stream(std::istream &input, const std::string &input_name,
                    std::ostream &output,
                    const std::span<const std::byte> decryption_key) {
  // https://github.com/fivetran/fivetran_partner_sdk/blob/2f13d37849cc866ab71704158f5e9ba247b755b5/development-guide/destination-connector-development-guide.md#encryption
  // "Each batch file is encrypted separately using AES-256 in CBC mode and with
  // PKCS5Padding. You can find the encryption key for each batch file in the
  // WriteBatchRequest#keys field. First 16 bytes of each batch file hold the
  // IV vector."

  if (decryption_key.size() != 32) {
    throw std::invalid_argument(
        "Decryption key must be 32 bytes long for AES-256-CBC");
  }

  // The first 16 bytes of the input stream is the IV
  const std::vector<std::byte> iv = read_iv(input, input_name);

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (ctx == nullptr) {
    openssl_helper::raise_openssl_error(
        "Failed to create decryption cipher context");
  }
  openssl_helper::CipherCtxDeleter ctx_deleter(ctx);

  initialize_cipher_context(ctx, input_name, iv, decryption_key);

  // Disable exceptions thrown during writes. We do our own error handling.
  output.exceptions(std::ios::goodbit);
  decrypt_stream_content(input, input_name, output, ctx);
}

#pragma GCC diagnostic pop

void decrypt_file(const std::string &filename, std::ostream &output,
                  const std::span<const std::byte> decryption_key) {
  std::ifstream input_file_stream(filename, std::ios::binary);
  if (!input_file_stream) {
    throw std::system_error(errno, std::iostream_category(),
                            "Failed to open encrypted file " + filename);
  }
  // Disable exceptions thrown during writes. We do our own error handling.
  input_file_stream.exceptions(std::ios::goodbit);

  decrypt_stream(input_file_stream, filename, output, decryption_key);
}
} // namespace decryption
