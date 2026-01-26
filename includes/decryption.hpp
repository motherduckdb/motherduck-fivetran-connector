#pragma once

#include <cstddef>
#include <istream>
#include <ostream>
#include <span>
#include <string>

namespace decryption {
constexpr int AES_BLOCK_SIZE = 16;

/// Decrypts the provided file using AES-256-CBC with PKCS5 padding.
/// The `input_name` parameter is used to provide additional context in error
/// messages. Writes the decrypted content to the provided output stream.
void decrypt_stream(std::istream &input, const std::string &input_name,
                    std::ostream &output,
                    std::span<const std::byte> decryption_key);

/// Decrypts the provided file using AES-256-CBC with PKCS5 padding.
/// Writes the decrypted content to the provided output stream.
void decrypt_file(const std::string &filename, std::ostream &output,
                  std::span<const std::byte> decryption_key);
} // namespace decryption
