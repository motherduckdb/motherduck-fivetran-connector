#include <arrow/csv/api.h>

std::shared_ptr<arrow::Table>
ReadEncryptedCsv(const std::string &filename, const std::string *decryption_key,
                 std::vector<std::string> *utf8_columns);

std::shared_ptr<arrow::Table>
ReadUnencryptedCsv(const std::string &filename,
                   std::vector<std::string> *utf8_columns);