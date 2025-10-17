#include <string>
#include <vector>

std::vector<unsigned char> decrypt_stream(std::istream &input,
                                          const std::string &filename,
                                          const unsigned char *decryption_key);
std::vector<unsigned char> decrypt_file(const std::string &filename,
                                        const unsigned char *decryption_key);