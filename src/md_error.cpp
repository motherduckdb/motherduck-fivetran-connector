#include "md_error.hpp"

namespace md_error
{
    ExceptionWithTaskResolution::ExceptionWithTaskResolution(const std::string& msg) : runtime_error(msg) {}
} // namespace md_error
