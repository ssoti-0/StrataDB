#ifndef STRATADB_PARSER_H
#define STRATADB_PARSER_H

#include "query/ast.h"
#include <cstddef>
#include <string>
#include <vector>

namespace stratadb {

enum class TokenType {
    CREATE,
    TABLE,
    INSERT,
    INTO,
    VALUES,
    SELECT,
    FROM,
    WHERE,
    DELETE,
    INT,       
    PRIMARY,
    KEY,

     
    IDENTIFIER,  
    INTEGER,     

     
    LPAREN,      
    RPAREN,     
    COMMA,       
    EQUALS,      
    STAR,       
    SEMICOLON,   

    END_OF_INPUT
};

struct Token {
    TokenType type;
    std::string value;   
    std::size_t position; 
};

std::vector<Token> tokenize(const std::string& sql);

  class Parser {
  public:
    static Statement parse(const std::string& sql);

  private:
      explicit Parser(std::vector<Token> tokens);

    CreateTableStmt parse_create_table();
    InsertStmt parse_insert();
    SelectStmt parse_select();
    DeleteStmt parse_delete();

    const Token& current() const;
    const Token& advance();
    const Token& expect(TokenType expected, const std::string& context);
    bool check(TokenType type) const;

    bool match(TokenType type);

    [[noreturn]] void error(const std::string& message) const;

    std::vector<Token> tokens_;
    std::size_t pos_; 
};

} 

#endif
