#include "query/parser.h"

#include <cctype>
#include <stdexcept>
#include <unordered_map>
#include <algorithm>

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
    {"JOIN",    TokenType::JOIN},
    {"ON",      TokenType::ON},
};

static std::string token_type_name(TokenType type) {
    switch (type) {
        case TokenType::CREATE:       return "CREATE";
        case TokenType::TABLE:        return "TABLE";
        case TokenType::INSERT:       return "INSERT";
        case TokenType::INTO:         return "INTO";
        case TokenType::VALUES:       return "VALUES";
        case TokenType::SELECT:       return "SELECT";
        case TokenType::FROM:         return "FROM";
        case TokenType::WHERE:        return "WHERE";
        case TokenType::DELETE:       return "DELETE";
        case TokenType::INT:          return "INT";
        case TokenType::PRIMARY:      return "PRIMARY";
        case TokenType::KEY:          return "KEY";
        case TokenType::JOIN:         return "JOIN";
        case TokenType::ON:           return "ON";
        case TokenType::IDENTIFIER:   return "identifier";
        case TokenType::INTEGER:      return "integer";
        case TokenType::LPAREN:       return "'('";
        case TokenType::RPAREN:       return "')'";
        case TokenType::COMMA:        return "','";
        case TokenType::EQUALS:       return "'='";
        case TokenType::STAR:         return "'*'";
        case TokenType::DOT:          return "'.'";
        case TokenType::SEMICOLON:    return "';'";
        case TokenType::END_OF_INPUT: return "end of input";
    }
    return "unknown";
}


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
        case '(': tokens.push_back({TokenType::LPAREN,    "(", start}); i++; continue;
        case ')': tokens.push_back({TokenType::RPAREN,    ")", start}); i++; continue;
        case ',': tokens.push_back({TokenType::COMMA,     ",", start}); i++; continue;
        case '=': tokens.push_back({TokenType::EQUALS,    "=", start}); i++; continue;
        case '*': tokens.push_back({TokenType::STAR,      "*", start}); i++; continue;
        case '.': tokens.push_back({TokenType::DOT,       ".", start}); i++; continue;
        case ';': tokens.push_back({TokenType::SEMICOLON, ";", start}); i++; continue;
        default: break;
    }

    if (std::isdigit(static_cast<unsigned char>(sql[i])) ||
              (sql[i] == '-' && i + 1 < sql.size() &&
               std::isdigit(static_cast<unsigned char>(sql[i + 1])))) {
              if (sql[i] == '-') ++i;  // consume the minus
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
        std::string upper = word;
        std::transform(upper.begin(), upper.end(), upper.begin(),[](unsigned char c) { return std::toupper(c); });
        auto it = KEYWORDS.find(upper);


        if (it != KEYWORDS.end()) {
        tokens.push_back({it->second, word, start});
        } else {
        tokens.push_back({TokenType::IDENTIFIER, word, start});
        }
        continue;
        }

        throw std::runtime_error( "Lexer error at position " + std::to_string(i) + ": unexpected character '" + sql[i] + "'");
    }

    tokens.push_back({TokenType::END_OF_INPUT, "", sql.size()});
    return tokens;
}

Parser::Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)), pos_(0) {}

Statement Parser::parse(const std::string& sql) {
    auto tokens = tokenize(sql);
    Parser parser(std::move(tokens));

    Statement stmt;
    if (parser.check(TokenType::CREATE)) {
        stmt = parser.parse_create_table();
    } else if (parser.check(TokenType::INSERT)) {
        stmt = parser.parse_insert();
    } else if (parser.check(TokenType::SELECT)) {
        if (parser.tokens_.size() > 4 &&
            parser.tokens_[parser.pos_ + 4].type == TokenType::JOIN) {
            stmt = parser.parse_join_select();
        } else {
            stmt = parser.parse_select();
        }

    } else if (parser.check(TokenType::DELETE)) {
        stmt = parser.parse_delete();
    } else {
        parser.error("expected CREATE, INSERT, SELECT, or DELETE");
    }

    parser.match(TokenType::SEMICOLON);

    if (!parser.check(TokenType::END_OF_INPUT)) {
        parser.error("unexpected tokens after statement");
    }

    return stmt;
}
CreateTableStmt Parser::parse_create_table() {
    expect(TokenType::CREATE, "statement start");
    expect(TokenType::TABLE, "CREATE TABLE");

    const Token& name_tok = expect(TokenType::IDENTIFIER, "table name after CREATE TABLE");
    CreateTableStmt stmt;
    stmt.table_name = name_tok.value;

    expect(TokenType::LPAREN, "column definitions");

    const Token& key_col = expect(TokenType::IDENTIFIER,"primary key column name");
    stmt.key_column = key_col.value;
    expect(TokenType::INT, "column type (must be INT)");
    expect(TokenType::PRIMARY, "PRIMARY KEY constraint");
    expect(TokenType::KEY, "PRIMARY KEY constraint");

    expect(TokenType::COMMA, "between column definitions");

    const Token& val_col = expect(TokenType::IDENTIFIER, "value column name");
    stmt.value_column = val_col.value;
    expect(TokenType::INT, "column type (must be INT)");

    expect(TokenType::RPAREN, "end of column definitions");

    return stmt;
}
InsertStmt Parser::parse_insert() {
    expect(TokenType::INSERT, "statement start");
    expect(TokenType::INTO, "INSERT INTO");

    const Token& name_tok = expect(TokenType::IDENTIFIER,"table name after INSERT INTO");
    InsertStmt stmt;
    stmt.table_name = name_tok.value;

    expect(TokenType::VALUES, "VALUES clause");
    expect(TokenType::LPAREN, "value list");

    const Token& key_tok = expect(TokenType::INTEGER,"first value (key) in VALUES");
    stmt.key = std::stoi(key_tok.value);
    expect(TokenType::COMMA, "between values");

    const Token& val_tok = expect(TokenType::INTEGER,"second value (value) in VALUES");
    stmt.value = std::stoi(val_tok.value);

    expect(TokenType::RPAREN, "end of value list");

    return stmt;
}

SelectStmt Parser::parse_select() { 
    expect(TokenType::SELECT, "statement start");
    expect(TokenType::STAR, "SELECT *");
    expect(TokenType::FROM, "SELECT * FROM");

    const Token& name_tok = expect(TokenType::IDENTIFIER,"table name after FROM");
    SelectStmt stmt;
    stmt.table_name = name_tok.value;

    expect(TokenType::WHERE, "WHERE clause");
    expect(TokenType::IDENTIFIER, "column name in WHERE clause");
    expect(TokenType::EQUALS, "= in WHERE clause");

    const Token& val_tok = expect(TokenType::INTEGER,"value in WHERE clause");
    stmt.search_key = std::stoi(val_tok.value);

    return stmt;
}

DeleteStmt Parser::parse_delete() {
    expect(TokenType::DELETE, "statement start");
    expect(TokenType::FROM, "DELETE FROM");

    const Token& name_tok = expect(TokenType::IDENTIFIER,"table name after DELETE FROM");
    DeleteStmt stmt;
    stmt.table_name = name_tok.value;

    expect(TokenType::WHERE, "WHERE clause");
    expect(TokenType::IDENTIFIER, "column name in WHERE clause");
    expect(TokenType::EQUALS, "= in WHERE clause");

    const Token& val_tok = expect(TokenType::INTEGER,"value in WHERE clause");
    stmt.search_key = std::stoi(val_tok.value);

    return stmt;
}

const Token& Parser::current() const {
      return tokens_[pos_];
}

const Token& Parser::advance() {
    const Token& tok = tokens_[pos_];
    if (pos_ < tokens_.size() - 1) {
        ++pos_;
    }
    return tok;
}

const Token& Parser::expect(TokenType expected, const std::string& context) {
    if (current().type != expected) {
        throw std::runtime_error(
            "Parser error at position " +
            std::to_string(current().position) + ": expected " +
            token_type_name(expected) + " (" + context + "), got " +
            token_type_name(current().type) +
            (current().value.empty() ? "" : " '" + current().value + "'"));
    }
    return advance();
}

bool Parser::check(TokenType type) const {
return current().type == type;
}

bool Parser::match(TokenType type) {
    if (check(type)) {
        advance();
        return true;
    }
    return false;
}

void Parser::error(const std::string& message) const {
    throw std::runtime_error(
        "Parser error at position " + std::to_string(current().position) + ": " + message + (current().value.empty() ? "" : " (got '" + current().value + "')"));
}
} 