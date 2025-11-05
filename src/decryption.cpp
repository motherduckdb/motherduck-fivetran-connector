#include "decryption.hpp"
#include "openssl_helper.hpp"

#include <fstream>
#include <iostream>
#include <openssl/evp.h>
#include <system_error>

/// Decrypts the provided file using AES-256-CBC with PKCS5 padding.
/// The `input_name` parameter is used to provide additional context in error
/// messages. Returns the plaintext as a vector of bytes.
std::vector<unsigned char> decrypt_stream(std::istream &input,
                                          const std::string &input_name,
                                          std::ostream &output,
                                          const std::string &decryption_key) {
  // https://github.com/fivetran/fivetran_partner_sdk/blob/2f13d37849cc866ab71704158f5e9ba247b755b5/development-guide/destination-connector-development-guide.md#encryption
  // "Each batch file is encrypted separately using AES-256 in CBC mode and with
  // PKCS5Padding. You can find the encryption key for each batch file in the
  // WriteBatchRequest#keys field. First 16 bytes of each batch file hold the
  // IV vector."

  if (decryption_key.size() != 32) {
    throw std::runtime_error(
        "Decryption key must be 32 bytes long for AES-256-CBC");
  }

  constexpr int iv_length = 16;
  std::vector<unsigned char> iv(iv_length);
  input.read(reinterpret_cast<char *>(iv.data()), iv_length);
    std::streamsize bytes_read = input.gcount();
    assert(bytes_read == iv_length);

  if (input.fail()) {
    if (input.eof()) {
      throw std::runtime_error(
          "Unexpected end of file while reading IV from file " +
          input_name ". Read " + std::to_string(bytes_read) + " bytes");
    } else {
            throw std::system_error(errno, std::iostream_category(),
                                    "Error reading IV from file " + input_name ");
    }
  }

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (ctx == nullptr) {
    openssl_helper::raise_openssl_error(
        "Failed to create decryption cipher context");
  }
  openssl_helper::CipherCtxDeleter ctx_deleter(ctx);

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
      ctx, EVP_aes_256_cbc(), decryption_key.c_str(), iv.data());
  EVP_CIPHER_free(aes_impl);
  if (1 != init_result) {
    openssl_helper::raise_openssl_error(
        "Failed to initialize decryption context for file " + input_name);
  }

  constexpr int aes_block_size = 16;
  const int buffer_size = 65536; // 64 KiB
  unsigned char input_buffer[buffer_size];
  // "if padding is enabled the decrypted data buffer out passed to
  // EVP_DecryptUpdate() should have sufficient room for (inl +
  // cipher_block_size) bytes"
  unsigned char plaintext_buffer[buffer_size + aes_block_size];
  int plaintext_length;

  // Disable exceptions thrown during writes. We do our own error handling.
  output.exceptions(0);

  // The istream bool operator evaluates to true if all requested bytes could be
  // read and no error occurred. The gcount() function returns the number of
  // bytes read by the last read operation.
  while (input.read(reinterpret_cast<char *>(input_buffer), buffer_size) ||
         input.gcount() > 0) {
    if (input.bad() || (input.fail() && !input.eof())) {
            throw std::system_error(errno, std::iostream_category(),
                                    "Error when reading input stream");
    }

    const auto bytes_read = static_cast<int>(input.gcount());
    if (1 != EVP_DecryptUpdate(ctx, plaintext_buffer, &plaintext_length,
                               input_buffer, bytes_read)) {
            openssl_helper::raise_openssl_error(
                "Could not decrypt UPDATE file " + input_name);
    }

    output.write(reinterpret_cast<char *>(plaintext_buffer), plaintext_length);
    if (!output) {
      throw std::system_error(errno, std::system_category(), error_msg);
    }
  }

  if (1 != EVP_DecryptFinal_ex(ctx, plaintext_buffer, &plaintext_length)) {
    openssl_helper::raise_openssl_error("Error during decryption finalization");
  }
  output.write(reinterpret_cast<char *>(plaintext_buffer), plaintext_length);

  int len = 0;
  std::vector<unsigned char> plaintext(encrypted_data.size());
  if (1 != EVP_DecryptUpdate(ctx, plaintext.data(), &len, encrypted_data.data(),
                             encrypted_data.size())) {
    openssl_helper::raise_openssl_error("Could not decrypt UPDATE file " +
                                        input_name);
  }
  int plaintext_len = len;
  if (1 != EVP_DecryptFinal_ex(ctx, plaintext.data() + len, &len)) {
    openssl_helper::raise_openssl_error(
        "Could not finalize decryption of file " + input_name);
  }
  plaintext_len += len;

  plaintext.resize(plaintext_len);

  return plaintext;
}

/// Decrypts the provided file using AES-256-CBC with PKCS5 padding.
/// Returns the plaintext as a vector of bytes.
std::vector<unsigned char> decrypt_file(const std::string &filename,
                                        const std::string &decryption_key) {
  std::ifstream file(filename, std::ios::binary);
  return decrypt_stream(file, filename, decryption_key);
}