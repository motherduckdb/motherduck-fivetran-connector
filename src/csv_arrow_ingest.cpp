#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/util/compression.h>
#include <csv_arrow_ingest.hpp>
#include <decryption.hpp>

arrow::csv::ConvertOptions
get_arrow_convert_options(std::vector<std::string> *utf8_columns) {
  auto convert_options = arrow::csv::ConvertOptions::Defaults();
  if (utf8_columns != nullptr) {
    // read all update-file CSV columns as text to accommodate
    // unmodified_string values
    for (auto &col_name : *utf8_columns) {
      convert_options.column_types.insert({col_name, arrow::utf8()});
    }
  }
  return convert_options;
}

std::shared_ptr<arrow::Table>
read_encrypted_csv(const std::string &filename, const std::string *decryption_key,
                   std::vector<std::string> *utf8_columns) {

  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = get_arrow_convert_options(utf8_columns);

  std::vector<unsigned char> plaintext = decrypt_file(
      filename,
      reinterpret_cast<const unsigned char *>(decryption_key->c_str()));
  auto buffer = std::make_shared<arrow::Buffer>(
      reinterpret_cast<const uint8_t *>(plaintext.data()), plaintext.size());
  auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);

  arrow::Compression::type compression_type = arrow::Compression::ZSTD;
  auto maybe_codec = arrow::util::Codec::Create(compression_type);
  if (!maybe_codec.ok()) {
    throw std::runtime_error(
        "Could not create codec from ZSTD compression type: " +
        maybe_codec.status().message());
  }
  auto codec = std::move(maybe_codec.ValueOrDie());
  auto maybe_compressed_input_stream =
      arrow::io::CompressedInputStream::Make(codec.get(), buffer_reader);
  if (!maybe_compressed_input_stream.ok()) {
    throw std::runtime_error("Could not input stream from compressed buffer: " +
                             maybe_compressed_input_stream.status().message());
  }
  auto compressed_input_stream =
      std::move(maybe_compressed_input_stream.ValueOrDie());
  auto maybe_table_reader = arrow::csv::TableReader::Make(
      arrow::io::default_io_context(), std::move(compressed_input_stream),
      read_options, parse_options, convert_options);
  if (!maybe_table_reader.ok()) {
    throw std::runtime_error(
        "Could not create table reader from decrypted file: " +
        maybe_table_reader.status().message());
  }
  auto table_reader = std::move(maybe_table_reader.ValueOrDie());

  auto maybe_table = table_reader->Read();
  if (!maybe_table.ok()) {
    throw std::runtime_error("Could not read CSV <" + filename +
                             ">: " + maybe_table.status().message());
  }
  auto table = std::move(maybe_table.ValueOrDie());

  return table;
}

std::shared_ptr<arrow::Table>
read_unencrypted_csv(const std::string &filename,
                     std::vector<std::string> *utf8_columns) {

  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = get_arrow_convert_options(utf8_columns);

  auto maybe_file =
      arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
  if (!maybe_file.ok()) {
    throw std::runtime_error("Could not open uncompressed file <" + filename +
                             ">: " + maybe_file.status().message());
  }
  auto plaintext_input_stream = std::move(maybe_file.ValueOrDie());
  auto maybe_table_reader = arrow::csv::TableReader::Make(
      arrow::io::default_io_context(), std::move(plaintext_input_stream),
      read_options, parse_options, convert_options);
  if (!maybe_table_reader.ok()) {
    throw std::runtime_error(
        "Could not create table reader from plaintext file: " +
        maybe_table_reader.status().message());
  }
  auto table_reader = std::move(maybe_table_reader.ValueOrDie());

  auto maybe_table = table_reader->Read();
  if (!maybe_table.ok()) {
    throw std::runtime_error("Could not read CSV <" + filename +
                             ">: " + maybe_table.status().message());
  }
  auto table = std::move(maybe_table.ValueOrDie());

  return table;
}
