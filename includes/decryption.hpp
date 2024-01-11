#include <openssl/evp.h>
#include <vector>
#include <string>

std::vector<unsigned char> decrypt_file(const std::string &filename,
                                        const unsigned char *decryption_key);