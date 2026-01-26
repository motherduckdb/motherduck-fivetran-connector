#include "memory_backed_file.hpp"

#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unistd.h>

#ifdef __linux__
#include <sys/mman.h>
#else
#include <sys/stat.h>
#endif

MemoryBackedFile MemoryBackedFile::Create(const size_t max_file_size) {
#ifdef __linux__
  // memfd_create creates an anonymous RAM-backed file
  // MFD_CLOEXEC closes the file descriptor on execve which prevents it from
  // leaking to child processes.
  const int fd =
      memfd_create("fivetran_decrypted.csv", MFD_CLOEXEC | MFD_ALLOW_SEALING);
  if (fd == -1) {
    throw std::system_error(errno, std::generic_category(),
                            "Failed to create memfd");
  }
#else
  // macOS: /tmp may or may not be RAM-backed.
  // This is fine because macOS is not used for production.
  const std::string tmp_dir = "/tmp/fivetran";

  if (mkdir(tmp_dir.c_str(), 0700) == -1 && errno != EEXIST) {
    throw std::system_error(errno, std::generic_category(),
                            "Failed to create temp directory " + tmp_dir);
  }

  // Template for mkstemp. XXXXXX will be replaced with unique characters.
  std::string tmp_path = tmp_dir + "/decrypted.csv.XXXXXX";
  const int fd = mkstemp(tmp_path.data());
  if (fd == -1) {
    throw std::system_error(errno, std::generic_category(),
                            "Failed to create temp memfile " + tmp_path);
  }

  // Remove file from filesystem immediately. Will be deleted when file
  // descriptor is closed.
  if (unlink(tmp_path.c_str()) == -1) {
    close(fd);
    throw std::system_error(errno, std::generic_category(),
                            "Failed to unlink temp memfile " + tmp_path);
  }
#endif

  MemoryBackedFile mem_file(fd, max_file_size);
  mem_file.Truncate(max_file_size);
  return mem_file;
}

MemoryBackedFile::MemoryBackedFile(MemoryBackedFile &&other) noexcept
    : fd(other.fd), path(std::move(other.path)) {
  other.fd = -1;
}

MemoryBackedFile &
MemoryBackedFile::operator=(MemoryBackedFile &&other) noexcept {
  if (this != &other) {
    if (fd >= 0) {
      close(fd);
    }
    fd = other.fd;
    path = std::move(other.path);
    other.fd = -1;
  }
  return *this;
}

MemoryBackedFile::~MemoryBackedFile() {
  if (fd >= 0) {
    close(fd);
  }
}

void MemoryBackedFile::Truncate(const size_t new_file_size) const {
  if (new_file_size > max_file_size) {
    throw std::invalid_argument(
        "Cannot increase size of MemoryBackedFile (max size is " +
        std::to_string(max_file_size) + " bytes)");
  }

  if (new_file_size > static_cast<size_t>(std::numeric_limits<off_t>::max())) {
    throw std::overflow_error("file_size exceeds maximum off_t value");
  }

  if (ftruncate(fd, static_cast<off_t>(new_file_size)) == -1) {
    throw std::system_error(errno, std::generic_category(),
                            "Failed to truncate memfile with fd=" +
                                std::to_string(fd));
  }
}
