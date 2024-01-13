#include <arrow/csv/api.h>

std::shared_ptr<arrow::Table>
read_encrypted_csv(const std::string &filename,
                   const std::string &decryption_key,
                   std::vector<std::string> &utf8_columns);

std::shared_ptr<arrow::Table>
read_unencrypted_csv(const std::string &filename,
                     std::vector<std::string> &utf8_columns);