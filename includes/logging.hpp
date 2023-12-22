#include <iostream>
#include <map>

namespace mdlog {

    void log(const std::string level, const std::string message) {
        std::cout << "{\"level\":\"" << level << "\","
                  << "\"message\":\"" << message << "\","
                  << "\"message-origin\":\"sdk_destination\"}"
                  << std::endl;
    }

    void info(std::string message) {
        log("INFO", message);
    }

    void warning(std::string message) {
        log("WARNING", message);
    }

    void severe(std::string message) {
        log("SEVERE", message);
    }
}
