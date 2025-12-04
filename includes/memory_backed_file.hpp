#pragma once

#include <string>

/// A RAM-backed file on Linux. On macOS, this file is located in the /tmp
/// directory. It is not visible in the filesystem, but accessible via its file
/// descriptor.
class MemoryBackedFile {
public:
  [[nodiscard]] static MemoryBackedFile Create(size_t file_size);

  ~MemoryBackedFile();

  MemoryBackedFile(const MemoryBackedFile &) = delete;
  MemoryBackedFile &operator=(const MemoryBackedFile &) = delete;
  MemoryBackedFile(MemoryBackedFile &&other) noexcept;
  MemoryBackedFile &operator=(MemoryBackedFile &&other) noexcept;

  int fd;
  // On BSD/OSX, the cursor is shared between file descriptors
  // (https://man.freebsd.org/cgi/man.cgi?fdescfs): "if the file descriptor is
  // open and the mode the file is being opened with is a subset of the
  // mode of the existing descriptor, the call: `fd = open("/dev/fd/0",
  // mode);` and the call: `fd = fcntl(0, F_DUPFD, 0);` are equivalent."
  std::string path;

private:
  // The file descriptor can be accessed via /dev/fd/<fd> on both Linux and
  // macOS
  explicit MemoryBackedFile(const int fd_)
      : fd(fd_), path("/dev/fd/" + std::to_string(fd_)) {}
};
