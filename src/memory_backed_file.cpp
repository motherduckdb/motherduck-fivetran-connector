#include "memory_backed_file.hpp"

#include <string>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>

MemoryBackedFile MemoryBackedFile::Create(const int file_size) {
#ifdef __linux__
  // /dev/shm is guaranteed tmpfs (in RAM) on Linux
  const std::string tmp_dir = "/dev/shm/fivetran";
#else
  // macOS: /tmp may or may not be RAM-backed.
  // This is fine because macOS is not used for production.
  const std::string tmp_dir = "/tmp/fivetran";
#endif

  if (mkdir(tmp_dir.c_str(), 0700) == -1 && errno != EEXIST) {
    throw std::system_error(errno, std::generic_category(),
                            "Failed to create temp directory " + tmp_dir);
  }

  // Template for mkstemp. XXXXXX will be replaced with unique characters.
  std::string tmp_path = tmp_dir + "/decrypted.csv.XXXXXX";
  int fd = mkstemp(tmp_path.data());
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

  if (ftruncate(fd, file_size) == -1) {
    close(fd);
    throw std::system_error(errno, std::generic_category(),
                            "Failed to set size of temp memfile " + tmp_path);
  }

  return MemoryBackedFile(fd);
}

MemoryBackedFile::~MemoryBackedFile() {
  if (fd >= 0) {
    close(fd);
  }
}
