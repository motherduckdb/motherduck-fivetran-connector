#pragma once

#include <optional>
#include <stdexcept>
#include <string>

namespace config {
inline constexpr const char* PROP_DATABASE = "motherduck_database";
inline constexpr const char* PROP_TOKEN = "motherduck_token";
inline constexpr const char* PROP_MAX_RECORD_SIZE = "max_record_size";
inline constexpr const char* PROP_STRICT_PRIMARY_KEYS = "strict_primary_keys";

/// Reads the property with name `property_name` from the config and throws if it is not found.
template <typename MapLike>
std::string find_property(const MapLike& config, const std::string& property_name) {
	const auto it = config.find(property_name);
	if (it == config.end()) {
		throw std::invalid_argument("Missing property " + property_name);
	}
	return it->second;
}

/// Reads the property with name `property_name` from the config or returns std::nullopt if it is not found.
template <typename MapLike>
std::optional<std::string> find_optional_property(const MapLike& config, const std::string& property_name) {
	const auto it = config.find(property_name);
	if (it == config.end()) {
		return std::nullopt;
	}
	return std::make_optional(it->second);
}

/// Reads a boolean-valued property (toggle field). Only the string "true" is interpreted as true.
/// A missing field or empty value resolves to the provided `default_value` to allow keeping the existing behavior
/// unchanged.
template <typename MapLike>
bool find_bool_property(const MapLike& config, const std::string& property_name, bool default_value) {
	const auto it = config.find(property_name);
	if (it == config.end() || it->second.empty()) {
		return default_value;
	}
	return it->second == "true";
}
} // namespace config
