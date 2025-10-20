#include "decryption.hpp"
#include "openssl_helper.hpp"

#include <fstream>
#include <openssl/evp.h>

/// Decrypts the provided file using AES-256-CBC with PKCS5 padding.
/// The `input_name` parameter is used to provide additional context in error
/// messages. Returns the plaintext as a vector of bytes.
std::vector<unsigned char> decrypt_stream(std::istream &input,
                                          const std::string &input_name,
                                          const unsigned char *decryption_key) {
  // https://github.com/fivetran/fivetran_partner_sdk/blob/2f13d37849cc866ab71704158f5e9ba247b755b5/development-guide/destination-connector-development-guide.md#encryption
  // "Each batch file is encrypted separately using AES-256 in CBC mode and with
  // PKCS5Padding. You can find the encryption key for each batch file in the
  // WriteBatchRequest#keys field. First 16 bytes of each batch file hold the
  // IV vector."

  constexpr int iv_length = 16;
  std::vector<unsigned char> iv(16);
  input.read(reinterpret_cast<char *>(iv.data()), iv_length);

  const std::vector<unsigned char> encrypted_data(
      std::istreambuf_iterator<char>{input}, {});

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (!ctx) {
    openssl_helper::raise_openssl_error(
        "Failed to create decryption cipher context");
  }
  openssl_helper::CipherCtxDeleter ctx_deleter(ctx);

  if (1 != EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), nullptr, decryption_key,
                              iv.data())) {
    openssl_helper::raise_openssl_error(
        "Failed to initialize decryption context for file " + input_name);
  }
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
                                        const unsigned char *decryption_key) {
  std::ifstream file(filename, std::ios::binary);
  return decrypt_stream(file, filename, decryption_key);
}