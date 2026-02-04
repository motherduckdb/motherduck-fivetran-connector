#pragma once

#include <stdexcept>
#include <string>

namespace config {
inline constexpr const char* PROP_DATABASE = "motherduck_database";
inline constexpr const char* PROP_TOKEN = "motherduck_token";

template <typename MapLike>
std::string find_property(const MapLike& config, const std::string& property_name) {
	const auto token_it = config.find(property_name);
	if (token_it == config.end()) {
		throw std::invalid_argument("Missing property " + property_name);
	}
	return token_it->second;
}
} // namespace config
