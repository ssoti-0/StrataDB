#include "query/parser.h"

#include <cctype>
#include <stdexcept>
#include <unordered_map>

namespace stratadb {


static const std::unordered_map<std::string, TokenType> KEYWORDS = {
    {"CREATE",  TokenType::CREATE},
    {"TABLE",   TokenType::TABLE},
    {"INSERT",  TokenType::INSERT},
    {"INTO",    TokenType::INTO},
    {"VALUES",  TokenType::VALUES},
    {"SELECT",  TokenType::SELECT},
    {"FROM",    TokenType::FROM},
    {"WHERE",   TokenType::WHERE},
    {"DELETE",  TokenType::DELETE},
    {"INT",     TokenType::INT},
    {"PRIMARY", TokenType::PRIMARY},
    {"KEY",     TokenType::KEY},
};

std::vector<Token> tokenize(const std::string& sql) {
    std::vector<Token> tokens;
    std::size_t i = 0;

    while (i < sql.size()) {
    if (std::isspace(static_cast<unsigned char>(sql[i]))) {
        ++i;
        continue;
    }

    std::size_t start = i;

    switch (sql[i]) {
        case '(': tokens.push_back({TokenType::LPAREN,    "(", start}); ++i; continue;
        case ')': tokens.push_back({TokenType::RPAREN,    ")", start}); ++i; continue;
        case ',': tokens.push_back({TokenType::COMMA,     ",", start}); ++i; continue;
        case '=': tokens.push_back({TokenType::EQUALS,    "=", start}); ++i; continue;
        case '*': tokens.push_back({TokenType::STAR,      "*", start}); ++i; continue;
        case ';': tokens.push_back({TokenType::SEMICOLON, ";", start}); ++i; continue;
        default: break;
    }

    if (std::isdigit(static_cast<unsigned char>(sql[i]))) {
        while (i < sql.size() &&
            std::isdigit(static_cast<unsigned char>(sql[i]))) {
            ++i;
        }
        tokens.push_back({TokenType::INTEGER,
            sql.substr(start, i - start), start});
            continue;
        }
        if (std::isalpha(static_cast<unsigned char>(sql[i])) || sql[i] == '_') {
            while (i < sql.size() && (std::isalnum(static_cast<unsigned char>(sql[i])) || sql[i] == '_')) {
            ++i;
        }
        std::string word = sql.substr(start, i - start);

        auto it = KEYWORDS.find(word);
        if (it != KEYWORDS.end()) {
        tokens.push_back({it->second, word, start});
        } else {
        tokens.push_back({TokenType::IDENTIFIER, word, start});
        }
        continue;
        }

        throw std::runtime_error(
        "Lexer error at position " + std::to_string(i) +
        ": unexpected character '" + sql[i] + "'");
    }

    tokens.push_back({TokenType::END_OF_INPUT, "", sql.size()});
    return tokens;
}

} 