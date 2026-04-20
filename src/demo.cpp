#include "query/parser.h"                                                                         
#include "execution/executor.h"                           

#include <iostream>
#include <string>

int main() {
    std::cout << "StrataDB Interactive Shell\n";
    std::cout << "Type SQL commands, or 'quit' to exit.\n";
    std::cout << "Special commands: STATS, VERBOSE ON/OFF, BENCHMARK\n\n";

    stratadb::Executor exec("data");

    std::string line;
    while (true) {
        std::cout << "stratadb> " << std::flush;
        if (!std::getline(std::cin, line)) break;
        if (line.empty()) continue;
        if (line == "quit" || line == "exit") break;

        try {
            auto stmt = stratadb::Parser::parse(line);
            std::cout << exec.execute(stmt) << "\n";
        } catch (const std::exception& e) {
            std::cout << "Error: " << e.what() << "\n";
        }
    }
    return 0;
}
