#ifndef STRATADB_PARSER_H
#define STRATADB_PARSER_H

#include "include/query/ast.h"
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
    std::string value;   // original text (e.g. "INSERT", "my_table", "42")
    std::size_t position; // character offset in the input string
};

#endif
