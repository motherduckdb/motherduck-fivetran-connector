#pragma once

#include <string>

#define STRING(x) #x
#define XSTRING(s) STRING(s)
const std::string TEST_RESOURCES_DIR = XSTRING(TEST_RESOURCES_LOCATION);

static const std::string TEST_DATABASE_NAME = "fivetran_test010";