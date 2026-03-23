#ifndef STRATADB_AST_H
#define STRATADB_AST_H

#include <cstdint>
#include <string>
#include <variant>

struct CreateTableStmt {
    std::string table_name;
    std::string key_column;    
    std::string value_column;  
};

struct InsertStmt {
    std::string table_name;
    int32_t key;
    int32_t value;
};

struct SelectStmt {
    std::string table_name;
    int32_t search_key;
};

struct DeleteStmt {
    std::string table_name;
    int32_t search_key;
};

using Statement = std::variant<CreateTableStmt, InsertStmt, SelectStmt, DeleteStmt>;

}

#endif 
