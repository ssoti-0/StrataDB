#ifndef STRATADB_EXECUTOR_H
#define STRATADB_EXECUTOR_H

#include "index/btree.h"
#include "query/ast.h"
#include "storage/disk_manager.h"

#include <string>

namespace stratadb {

static constexpr std::size_t MAX_IDENTIFIER_LENGTH = 63;

struct Schema {
    bool initialized = false;
    std::string table_name;
    std::string key_column;
    std::string value_column;
};

class Executor {
public:
    explicit Executor(DiskManager& disk_manager);
    std::string execute(const Statement& stmt);

private:
    std::string execute_create(const CreateTableStmt& stmt);
    std::string execute_insert(const InsertStmt& stmt);
    std::string execute_select(const SelectStmt& stmt);
    std::string execute_delete(const DeleteStmt& stmt);
    Schema read_schema() const;
    void write_schema(const Schema& schema);
    void require_table(const std::string& table_name) const;
    
    DiskManager& disk_manager_;
    BPlusTree tree_;
    Schema schema_;
  };

} 

#endif 