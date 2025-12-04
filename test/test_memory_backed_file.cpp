#include "memory_backed_file.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <filesystem>
#include <fstream>
#include <vector>


TEST_CASE("MemoryBackedFile::Create gives valid file constructor", "[memory_backed_file]") {
  constexpr int file_size = 512;
  auto memfile = MemoryBackedFile::Create(file_size);

  REQUIRE(memfile.fd >= 0);
  REQUIRE(std::filesystem::file_size(memfile.path) == file_size);
}

TEST_CASE("MemoryBackedFile with zero size is valid", "[memory_backed_file]") {
  auto memfile = MemoryBackedFile::Create(0);
  REQUIRE(memfile.fd >= 0);
}

TEST_CASE("MemoryBackedFile is zero-filled after creation", "[memory_backed_file]") {
  constexpr int file_size = 4096;
  auto memfile = MemoryBackedFile::Create(file_size);

  std::ifstream in(memfile.path, std::ios::binary);
  REQUIRE(in.is_open());

  std::vector<char> buffer(file_size);
  in.read(buffer.data(), file_size);
  REQUIRE(in.gcount() == file_size);

  for (int i = 0; i < file_size; ++i) {
    REQUIRE(buffer[i] == 0);
  }
}

TEST_CASE("MemoryBackedFile can be written to and read from", "[memory_backed_file]") {
  const std::string test_data = "Hello, MemoryBackedFile!";
  auto memfile = MemoryBackedFile::Create(test_data.size() + 1);

  {
    std::ofstream out(memfile.path, std::ios::binary);
    REQUIRE(out.is_open());
    out << test_data << std::endl;
  }

  {
    std::ifstream in(memfile.path, std::ios::binary);
    REQUIRE(in.is_open());

    // On BSD/OSX, the cursor is shared between file descriptors: https://man.freebsd.org/cgi/man.cgi?fdescfs
    // "if the file descriptor is open	and the	mode the file is being opened with is a	subset
    // of the mode of the existing descriptor, the call:
    // fd	= open("/dev/fd/0", mode);
    // and the call:
    // fd	= fcntl(0, F_DUPFD, 0);
    // are equivalent."
#ifdef __APPLE__
    in.seekg(0);
#endif

    std::string result;
    std::getline(in, result);
    REQUIRE(result == test_data);
  }
}

TEST_CASE("MemoryBackedFile growth in size when writing many bytes", "[memory_backed_file]") {
  auto memfile = MemoryBackedFile::Create(10);

  const std::string test_data = "This data exceeds the initial size of the MemoryBackedFile.";

  {
    std::ofstream out(memfile.path);
    REQUIRE(out.is_open());
    out << test_data << std::endl;
    REQUIRE(out.bad() == false);
  }

  {
    std::ifstream in(memfile.path);
    REQUIRE(in.is_open());
#ifdef __APPLE__
    in.seekg(0);
#endif

    std::string result;
    std::getline(in, result);
    // The read should be truncated to the first 10 bytes
    REQUIRE(result == test_data);
  }
}

TEST_CASE("MemoryBackedFile is not visible in filesystem", "[memory_backed_file]") {
  auto memfile = MemoryBackedFile::Create(256);

// The underlying temp file is unlinked immediately after creation.
// On Linux, it was in /dev/shm/fivetran/; on macOS, in /tmp/fivetran/
#ifdef __linux__
  const std::string tmp_dir = "/dev/shm/fivetran";
#else
  const std::string tmp_dir = "/tmp/fivetran";
#endif

  if (!std::filesystem::exists(tmp_dir)) {
    return;
  }

  REQUIRE(std::filesystem::is_empty(tmp_dir));
  // But the file is still accessible via the /dev/fd path
  REQUIRE(std::filesystem::exists(memfile.path));
}

TEST_CASE("MemoryBackedFile is temporary", "[memory_backed_file]") {
  std::string captured_path;
  {
    auto memfile = MemoryBackedFile::Create(256);
    captured_path = memfile.path;
    REQUIRE(std::filesystem::exists(captured_path));
  }

  // After destruction, the memfile should no longer be accessible
  REQUIRE_THROWS_WITH(std::filesystem::exists(captured_path), Catch::Matchers::ContainsSubstring("Bad file descriptor"));
}

TEST_CASE("Multiple MemoryBackedFiles can coexist", "[memory_backed_file]") {
  auto memfile1 = MemoryBackedFile::Create(1024);
  auto memfile2 = MemoryBackedFile::Create(2048);
  auto memfile3 = MemoryBackedFile::Create(512);

  // All memfiles should have different file descriptors
  REQUIRE(memfile1.fd != memfile2.fd);
  REQUIRE(memfile2.fd != memfile3.fd);
  REQUIRE(memfile1.fd != memfile3.fd);

  // Each memfile should have correct size
  REQUIRE(std::filesystem::file_size(memfile1.path) == 1024);
  REQUIRE(std::filesystem::file_size(memfile2.path) == 2048);
  REQUIRE(std::filesystem::file_size(memfile3.path) == 512);
}
