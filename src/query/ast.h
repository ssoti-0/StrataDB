#ifndef STRATADB_AST_H
#define STRATADB_AST_H

#include <cstdint>
#include <string>
#include <variant>

namespace stratadb {
struct CreateTableStmt {
    std::string table_name;
    std::string key_column;    
    std::string value_column;  
    std::string value_type;
};

struct InsertStmt {
    std::string table_name;
    int32_t key;
    std::string value;
};

struct SelectStmt {
    std::string table_name;
    int32_t search_key;
};

struct DeleteStmt {
    std::string table_name;
    int32_t search_key;
};

struct SelectAllStmt {
    std::string table_name;
};

struct JoinSelectStmt {
    std::string left_table;
    std::string right_table;
    std::string left_column;   
    std::string right_column;  
};

struct StatsStmt {};

struct VerboseStmt {
    bool enable;  // true = ON, false = OFF
};

struct BenchmarkStmt {};

using Statement = std::variant<CreateTableStmt, InsertStmt, SelectStmt, DeleteStmt, SelectAllStmt, JoinSelectStmt, StatsStmt, VerboseStmt, BenchmarkStmt>;

}

#endif 
