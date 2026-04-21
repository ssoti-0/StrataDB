#include "query/parser.h"

#include <cctype>
#include <stdexcept>
#include <unordered_map>
#include <algorithm>

namespace stratadb {


static const std::unordered_map<std::string, TokenType> KEYWORDS = {
    {"CREATE",TokenType::CREATE},
    {"TABLE",TokenType::TABLE},
    {"INSERT",TokenType::INSERT},
    {"INTO", TokenType::INTO},
    {"VALUES",TokenType::VALUES},
    {"SELECT",TokenType::SELECT},
    {"FROM", TokenType::FROM},
    {"WHERE",TokenType::WHERE},
    {"DELETE",TokenType::DELETE},
    {"INT",TokenType::INT},
    {"PRIMARY",TokenType::PRIMARY},
    {"KEY", TokenType::KEY},
    {"JOIN",TokenType::JOIN},
    {"ON", TokenType::ON},
    {"TEXT", TokenType::TEXT},
    {"STATS",TokenType::STATS},
    {"VERBOSE",TokenType::VERBOSE},
    {"BENCHMARK",TokenType::BENCHMARK},
};

static std::string token_type_name(TokenType type) {
    switch (type) {
        case TokenType::CREATE:return "CREATE";
        case TokenType::TABLE:return "TABLE";
        case TokenType::INSERT:return "INSERT";
        case TokenType::INTO:return "INTO";
        case TokenType::VALUES:return "VALUES";
        case TokenType::SELECT:return "SELECT";
        case TokenType::FROM:return "FROM";
        case TokenType::WHERE:return "WHERE";
        case TokenType::DELETE:return "DELETE";
        case TokenType::INT:return "INT";
        case TokenType::PRIMARY:return "PRIMARY";
        case TokenType::KEY:return "KEY";
        case TokenType::JOIN:return "JOIN";
        case TokenType::ON:return "ON";
        case TokenType::TEXT:return "TEXT";
        case TokenType::STATS:return "STATS";
        case TokenType::VERBOSE:return "VERBOSE";
        case TokenType::BENCHMARK:return "BENCHMARK";
        case TokenType::IDENTIFIER:return "identifier";
        case TokenType::INTEGER:return "integer";
        case TokenType::STRING:return "string";
        case TokenType::LPAREN:return "'('";
        case TokenType::RPAREN:return "')'";
        case TokenType::COMMA:return "','";
        case TokenType::EQUALS:return "'='";
        case TokenType::STAR:return "'*'";
        case TokenType::DOT:return "'.'";
        case TokenType::SEMICOLON:return "';'";
        case TokenType::END_OF_INPUT:return "end of input";
    }
    return "unknown";
}

// turn raw sql string into tokens 
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
        case '(': tokens.push_back({TokenType::LPAREN,"(", start}); i++; continue;
        case ')': tokens.push_back({TokenType::RPAREN,")", start}); i++; continue;
        case ',': tokens.push_back({TokenType::COMMA,",", start}); i++; continue;
        case '=': tokens.push_back({TokenType::EQUALS,"=", start}); i++; continue;
        case '*': tokens.push_back({TokenType::STAR,"*", start}); i++; continue;
        case '.': tokens.push_back({TokenType::DOT,".", start}); i++; continue;
        case ';': tokens.push_back({TokenType::SEMICOLON,";", start}); i++; continue;
        default: break;
    }

    if (sql[i] == '\'') {
        i++;  
        while (i < sql.size() && sql[i] != '\'') {
            ++i;
        }
        if (i >= sql.size()) {
            throw std::runtime_error("Lexer error: unterminated string literal");
        }
        std::string str_val = sql.substr(start + 1, i - start - 1);
        i++;  
        tokens.push_back({TokenType::STRING, str_val, start});
        continue;
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
      if (parser.tokens_.size() > 4 && parser.tokens_[parser.pos_ + 4].type == TokenType::JOIN) {
          stmt = parser.parse_join();
      } else if (parser.tokens_.size() > 4 && parser.tokens_[parser.pos_ + 4].type == TokenType::WHERE) {
          stmt = parser.parse_select();
      } else {
          stmt = parser.parse_select_all();
      }
    } else if (parser.check(TokenType::DELETE)) {
        stmt = parser.parse_delete();
    } else if (parser.check(TokenType::STATS)) {
          parser.advance();
          stmt = StatsStmt{};
    } else if (parser.check(TokenType::VERBOSE)) {
        parser.advance();
        // ON is a keyword token, OFF is an identifier — handle both
        if (parser.check(TokenType::ON)) {
            parser.advance();
            stmt = VerboseStmt{true};
        } else if (parser.check(TokenType::IDENTIFIER)) {
            std::string upper = parser.current().value;
            std::transform(upper.begin(), upper.end(), upper.begin(),[](unsigned char c) { return std::toupper(c); });
            if (upper == "OFF") {
            parser.advance();
            stmt = VerboseStmt{false};
        } else {
            parser.error("expected ON or OFF after VERBOSE");
        }
        } else {
            parser.error("expected ON or OFF after VERBOSE");
        }
    } else if (parser.check(TokenType::BENCHMARK)) {
        parser.advance();
        stmt = BenchmarkStmt{};
    } else {
        parser.error("expected CREATE, INSERT, SELECT, DELETE, STATS, VERBOSE, or BENCHMARK");
    }

    parser.match(TokenType::SEMICOLON);

    if (!parser.check(TokenType::END_OF_INPUT)) {
        parser.error("unexpected tokens after statement");
    }

    return stmt;
}
CreateTableStmt Parser::parse_create_table() {
    expect(TokenType::CREATE, "CREATE");
    expect(TokenType::TABLE, "CREATE TABLE");
    const Token& name_tok = expect(TokenType::IDENTIFIER, "table name after CREATE TABLE");
    CreateTableStmt stmt;
    stmt.table_name = name_tok.value;

    expect(TokenType::LPAREN, "opening paren");

    const Token& key_col = expect(TokenType::IDENTIFIER,"primary key column name");
    stmt.key_column = key_col.value;
    expect(TokenType::INT, "INT type for primary key");
    expect(TokenType::PRIMARY, "PRIMARY KEY");
    expect(TokenType::KEY, "KEY");
    expect(TokenType::COMMA, "comma");
    const Token& val_col = expect(TokenType::IDENTIFIER, "value column name");
    stmt.value_column = val_col.value;
    if (check(TokenType::INT)) {
      advance();
      stmt.value_type = "INT";
    } else if (check(TokenType::TEXT)) {
      advance();
      stmt.value_type = "TEXT";
    } else {
      error("expected INT or TEXT for column type");
    }

    expect(TokenType::RPAREN, "closing paren");

    return stmt;
}
InsertStmt Parser::parse_insert() {
    expect(TokenType::INSERT, "INSERT");
    expect(TokenType::INTO, "INSERT INTO");
    const Token& name_tok = expect(TokenType::IDENTIFIER,"table name after INSERT INTO");
    InsertStmt stmt;
    stmt.table_name = name_tok.value;
    expect(TokenType::VALUES, "VALUES");
    do {
        expect(TokenType::LPAREN, "opening paren");
        const Token& key_tok = expect(TokenType::INTEGER,"key value");
        int32_t key = std::stoi(key_tok.value);
        expect(TokenType::COMMA, "between values");

        std::string value;
        if (check(TokenType::INTEGER)) {
            value = advance().value;
        } else if (check(TokenType::STRING)) {
            value = advance().value;
        
        } else {
            error("expected integer or string value");
        }
        expect(TokenType::RPAREN, "closing paren");
        stmt.rows.emplace_back(key, value);
    } while (match(TokenType::COMMA));
    return stmt;
}

SelectStmt Parser::parse_select() { 
    expect(TokenType::SELECT, "SELECT");
    expect(TokenType::STAR, "SELECT *");
    expect(TokenType::FROM, "SELECT * FROM");
    const Token& name_tok = expect(TokenType::IDENTIFIER,"table name after FROM");
    SelectStmt stmt;
    stmt.table_name = name_tok.value;

    expect(TokenType::WHERE, "WHERE clause");
    expect(TokenType::IDENTIFIER, "column name in WHERE");
    expect(TokenType::EQUALS, "= in WHERE");

    const Token& val_tok = expect(TokenType::INTEGER,"value in WHERE clause");
    stmt.search_key = std::stoi(val_tok.value);

    return stmt;
}

DeleteStmt Parser::parse_delete() {
    expect(TokenType::DELETE, "DELETE");
    expect(TokenType::FROM, "DELETE FROM");

    const Token& name_tok = expect(TokenType::IDENTIFIER,"table name after DELETE FROM");
    DeleteStmt stmt;
    stmt.table_name = name_tok.value;
    expect(TokenType::WHERE, "WHERE clause");
    expect(TokenType::IDENTIFIER, "column name in WHERE");
    expect(TokenType::EQUALS, "= in WHERE clause");

    const Token& val_tok = expect(TokenType::INTEGER,"value in WHERE clause");
    stmt.search_key = std::stoi(val_tok.value);

    return stmt;
}

 SelectAllStmt Parser::parse_select_all() {
    expect(TokenType::SELECT, "SELECT");
    expect(TokenType::STAR, "SELECT *");
    expect(TokenType::FROM, "SELECT * FROM");
    const Token& name_tok = expect(TokenType::IDENTIFIER, "table name after FROM");
    SelectAllStmt stmt;
    stmt.table_name = name_tok.value;
    return stmt;
}

JoinSelectStmt Parser::parse_join() {
    expect(TokenType::SELECT, "SELECT");
    expect(TokenType::STAR, "SELECT *");
    expect(TokenType::FROM, "SELECT * FROM");

    const Token& left_table = expect(TokenType::IDENTIFIER,"left table name");
    expect(TokenType::JOIN, "JOIN");
    const Token& right_table = expect(TokenType::IDENTIFIER,"right table name");
    expect(TokenType::ON, "ON");
    const Token& on_left_table = expect(TokenType::IDENTIFIER,"table in ON");
    expect(TokenType::DOT, "dot");
    const Token& on_left_col = expect(TokenType::IDENTIFIER,"column in ON");
    expect(TokenType::EQUALS, "= in ON");
    const Token& on_right_table = expect(TokenType::IDENTIFIER,"table in ON clause");
    expect(TokenType::DOT, "dot in table.column");
    const Token& on_right_col = expect(TokenType::IDENTIFIER,"column in ON");

    JoinSelectStmt stmt;
    stmt.left_table = left_table.value;
    stmt.right_table = right_table.value;
    if (on_left_table.value == left_table.value) {
        stmt.left_column = on_left_col.value;
        stmt.right_column = on_right_col.value;
    } else {
        stmt.left_column = on_right_col.value;
        stmt.right_column = on_left_col.value;
    }
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
           "parse error at " + std::to_string(current().position) + ": expected " + token_type_name(expected) + " (" + context + ")");
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