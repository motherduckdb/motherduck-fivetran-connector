#pragma once

namespace crash_handler {

// Installs handlers for the fatal signals (SIGSEGV, SIGABRT, ...)
// and for orderly termination (SIGTERM, SIGINT).
void Install();

} // namespace crash_handler
