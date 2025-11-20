#pragma once

#include <string>

namespace test::constants {
#define STRING(x) #x
#define XSTRING(s) STRING(s)
const std::string TEST_RESOURCES_DIR = XSTRING(TEST_RESOURCES_LOCATION);
#undef XSTRING
#undef STRING

extern const std::string TEST_DATABASE_NAME;

extern const std::string MD_TOKEN;
} // namespace test::constants