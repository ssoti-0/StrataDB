#include "query/parser.h"                                                                         
#include "execution/executor.h"                           

#include <iostream>
#include <string>

int main() {
    std::cout << "StrataDB Interactive Shell\n";
    std::cout << "Type SQL commands, or 'quit' to exit.\n";
    std::cout << "All commands end with a semicolon (;)\n";                                           
    std::cout << "Special commands: STATS, VERBOSE ON/OFF, BENCHMARK\n\n";

    stratadb::Executor exec("data");

     std::string buffer;
      while (true) {
          if (buffer.empty())
              std::cout << "stratadb> " << std::flush;
          else
              std::cout << "       -> " << std::flush;

          std::string line;
          if (!std::getline(std::cin, line)) break;
          if (line.empty()) continue;
          if (line == "quit" || line == "exit") break;

          buffer += " " + line;

          if (buffer.find(';') != std::string::npos) {
              try {
                  auto stmt = stratadb::Parser::parse(buffer);
                  std::cout << exec.execute(stmt) << "\n";
              } catch (const std::exception& e) {
                  std::cout << "Error: " << e.what() << "\n";
              }
              buffer.clear();
          }
      }
      return 0;

}
