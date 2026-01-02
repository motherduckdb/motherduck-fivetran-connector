#pragma once

#include <stdexcept>
#include <string>


namespace md_error
{
class RecoverableError : public std::runtime_error {
    // There are several failure modes that require the exception to be turned into an actionable task instead of an
    // error. A custom exception class allows us to explicitly catch the errors we want to turn into a task.
public:
    explicit RecoverableError(const std::string& msg);
};
} // namespace md_error