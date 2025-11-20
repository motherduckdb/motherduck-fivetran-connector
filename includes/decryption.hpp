#include <iostream>
#include <string>

void decrypt_stream(std::istream &input, const std::string &input_name,
                    std::ostream &output, const std::string &decryption_key);

std::string decrypt_file(const std::string &filename,
                         const std::string &decryption_key);