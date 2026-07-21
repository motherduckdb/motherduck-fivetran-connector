#pragma once

#include <optional>
#include <stdexcept>
#include <string>

namespace config {
inline constexpr const char* PROP_DATABASE = "motherduck_database";
inline constexpr const char* PROP_TOKEN = "motherduck_token";
inline constexpr const char* PROP_MAX_RECORD_SIZE = "max_record_size";
inline constexpr const char* PROP_STRICT_PRIMARY_KEYS = "strict_primary_keys";

template <typename MapLike>
std::string find_property(const MapLike& config, const std::string& property_name) {
	const auto token_it = config.find(property_name);
	if (token_it == config.end()) {
		throw std::invalid_argument("Missing property " + property_name);
	}
	return token_it->second;
}

template <typename MapLike>
std::optional<std::string> find_optional_property(const MapLike& config, const std::string& property_name) {
	const auto token_it = config.find(property_name);
	if (token_it == config.end()) {
		return std::nullopt;
	}
	return std::make_optional(token_it->second);
}

/// Reads a boolean-valued property. Fivetran toggle fields serialize as the
/// strings "true"/"false"; anything other than "true" (including a missing or
/// empty value) resolves to `default_value`.
template <typename MapLike>
bool find_bool_property(const MapLike& config, const std::string& property_name, const bool default_value) {
	const auto it = config.find(property_name);
	if (it == config.end() || it->second.empty()) {
		return default_value;
	}
	return it->second == "true";
}
} // namespace config
