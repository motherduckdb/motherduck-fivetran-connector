#include <decryption.hpp>
#include <fstream>

std::vector<unsigned char> decrypt_file(const std::string &filename,
                                        const unsigned char *decryption_key) {

  std::ifstream file(filename, std::ios::binary);
  std::vector<unsigned char> iv(16);
  file.read(reinterpret_cast<char *>(iv.data()), iv.size());

  std::vector<unsigned char> encrypted_data(
      std::istreambuf_iterator<char>{file}, {});

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (!ctx) {
    throw std::runtime_error("Could not initialize decryption context");
  }
  if (1 != EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), NULL, decryption_key,
                              iv.data())) {
    throw std::runtime_error("Could not decrypt file " + filename);
  }
  int len;
  std::vector<unsigned char> plaintext(encrypted_data.size());
  if (1 != EVP_DecryptUpdate(ctx, plaintext.data(), &len, encrypted_data.data(),
                             encrypted_data.size())) {
    throw std::runtime_error("Could not decrypt UPDATE file " + filename);
  }
  int plaintext_len = len;
  if (1 != EVP_DecryptFinal_ex(ctx, plaintext.data() + len, &len)) {
    throw std::runtime_error("Could not decrypt FINAL file " + filename);
  }
  plaintext_len += len;
  EVP_CIPHER_CTX_free(ctx);

  plaintext.resize(plaintext_len);

  return plaintext;
}