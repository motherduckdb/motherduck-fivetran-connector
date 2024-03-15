#pragma once

#include <iostream>
#include <map>

namespace mdlog {

std::string escape_char(const std::string &str, const char &c);

void log(const std::string &level, const std::string &message);

void info(const std::string &message);

void warning(const std::string &message);

void severe(const std::string &message);

} // namespace mdlog
