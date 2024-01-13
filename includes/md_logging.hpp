#pragma once

#include <iostream>
#include <map>


namespace mdlog {

std::string escape_char(const std::string &str, const char &c);

void log(const std::string level, const std::string message);

void info(std::string message);

void warning(std::string message);

void severe(std::string message);

} // namespace mdlog
