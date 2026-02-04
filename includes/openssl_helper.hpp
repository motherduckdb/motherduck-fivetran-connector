#pragma once

#define OPENSSL_API_COMPAT 30000
#define OPENSSL_NO_DEPRECATED

#include <openssl/evp.h>
#include <string>

namespace openssl_helper {
void raise_openssl_error(const std::string &message_prefix);

/// RAII helper to free EVP_CIPHER and EVP_CIPHER_CTX
struct CipherCtxDeleter {
	EVP_CIPHER_CTX *cipher_ctx = nullptr;

	explicit CipherCtxDeleter(EVP_CIPHER_CTX *_cipher_ctx) : cipher_ctx(_cipher_ctx) {
	}

	~CipherCtxDeleter() {
		if (cipher_ctx) {
			EVP_CIPHER_CTX_free(cipher_ctx);
		}
	}
};
} // namespace openssl_helper
