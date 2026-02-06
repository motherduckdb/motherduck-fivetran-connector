#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>

namespace config {
inline constexpr const char *PROP_DATABASE = "motherduck_database";
inline constexpr const char *PROP_TOKEN = "motherduck_token";
inline constexpr const char *PROP_CSV_MAX_LINE_SIZE = "csv_max_line_size";

// We have to at some point handle up to eight parallel WriteBatch requests
// that all allocate a buffer of 4*max_line_size. The container memory limit is 1
// (or 2?) GiB. Assuming the worst case that all eight requests arrive at the
// same time, we need to limit the buffer size accordingly. We don't want to
// come too close to the limit, so we pick 768 MiB / 8 threads / 4 lines per thread = 24 MiB per line.
inline constexpr std::uint32_t DEFAULT_CSV_MAX_LINE_SIZE_BYTES = 24 * 1024 * 1024;

template <typename MapLike>
std::string find_property(const MapLike &config,
                          const std::string &property_name) {
  const auto token_it = config.find(property_name);
  if (token_it == config.end()) {
    throw std::invalid_argument("Missing property " + property_name);
  }
  return token_it->second;
}

template <typename MapLike>
std::optional<std::string> find_optional_property(const MapLike &config,
                                   const std::string &property_name) {
    const auto token_it = config.find(property_name);
    if (token_it == config.end()) {
        return std::nullopt;
    }
    return token_it->second;
}
} // namespace config
