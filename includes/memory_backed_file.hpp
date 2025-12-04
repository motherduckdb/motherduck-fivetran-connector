#pragma once

#include <string>

/// A RAM-backed file on Linux. On macOS, this file is located in the /tmp
/// directory. It is not visible in the filesystem, but accessible via its file
/// descriptor.
class MemoryBackedFile {
public:
  static MemoryBackedFile Create(int file_size);

  ~MemoryBackedFile();

  const int fd;
  const std::string path;

private:
  // The file descriptor can be accessed via /dev/fd/<fd> on both Linux and
  // macOS
  explicit MemoryBackedFile(const int fd_)
      : fd(fd_), path("/dev/fd/" + std::to_string(fd_)) {}
};
