#ifndef STRATADB_EXECUTOR_H
#define STRATADB_EXECUTOR_H

#include "index/btree.h"
#include "query/ast.h"
#include "storage/disk_manager.h"
#include <string>
#include <map>
#include <memory>

namespace stratadb {

// max length for table/column names
static constexpr std::size_t MAX_IDENTIFIER_LENGTH = 63;
// tablemetadata
struct Schema {
    bool initialized = false;
    std::string table_name;
    std::string key_column;
    std::string value_column;
};
// bundles everything a single table needs
struct TableInfo {
      std::unique_ptr<DiskManager> disk_manager;
      std::unique_ptr<BPlusTree> tree;
      Schema schema;
};
class Executor {
public:
    explicit Executor(const std::string& base_dir);
    std::string execute(const Statement& stmt);

private:

    std::string base_dir_;
    // per-table storage
    std::map<std::string, TableInfo> tables_;

    std::string execute_create(const CreateTableStmt& stmt);
    std::string execute_insert(const InsertStmt& stmt);
    std::string execute_select(const SelectStmt& stmt);
    std::string execute_delete(const DeleteStmt& stmt);
    std::string execute_select_all(const SelectAllStmt& stmt);
    std::string execute_join_select(const JoinSelectStmt& stmt);
    TableInfo& get_table(const std::string& name);
    // schema persistence in page 0 of each table's file
    Schema read_schema(DiskManager& dm) const;
    void write_schema(DiskManager& dm, const Schema& schema);
  };

} 
#endif 