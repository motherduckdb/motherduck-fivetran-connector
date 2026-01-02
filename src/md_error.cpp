#include "md_error.hpp"

namespace md_error {
RecoverableError::RecoverableError(const std::string &msg)
    : runtime_error(msg) {}
} // namespace md_error
