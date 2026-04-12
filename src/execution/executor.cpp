#include "execution/executor.h"
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <variant>

namespace stratadb {
// page 0 layout - bytes 0..3 belong to BPlusTree
static constexpr std::size_t SCHEMA_INIT_OFFSET = 4;
static constexpr std::size_t TABLE_NAME_OFFSET = 8;
static constexpr std::size_t KEY_COLUMN_OFFSET = 72;
static constexpr std::size_t VALUE_COLUMN_OFFSET = 136;
static constexpr std::size_t VALUE_TYPE_OFFSET = 200;  
static constexpr std::size_t IDENT_FIELD_SIZE = 64;

Executor::Executor(const std::string& base_dir) : base_dir_(base_dir) {
    namespace fs = std::filesystem;
    fs::create_directories(base_dir);
    // load existing tables from the directory
    for (const auto& entry : fs::directory_iterator(base_dir)) {
        if (entry.path().extension() != ".db") continue;
        std::string table_name = entry.path().stem().string();
        TableInfo handle;
        handle.disk_manager = std::make_unique<DiskManager>(entry.path().string());
        handle.tree = std::make_unique<BPlusTree>(*handle.disk_manager);
        handle.schema = read_schema(*handle.disk_manager);
        if (handle.schema.initialized) {
            tables_[table_name] = std::move(handle);
        }
    }
}

 std::string Executor::execute(const Statement& stmt) {
      if (auto* s = std::get_if<CreateTableStmt>(&stmt))
          return execute_create(*s);
      if (auto* s = std::get_if<InsertStmt>(&stmt))
          return execute_insert(*s);
      if (auto* s = std::get_if<SelectStmt>(&stmt))
          return execute_select(*s);
      if (auto* s = std::get_if<DeleteStmt>(&stmt))
          return execute_delete(*s);
      if (auto* s = std::get_if<SelectAllStmt>(&stmt))
          return execute_select_all(*s);
      if (auto* s = std::get_if<JoinSelectStmt>(&stmt))
          return execute_join_select(*s);
      throw std::runtime_error("unknown statement type");
 }

std::string Executor::execute_create(const CreateTableStmt& stmt) {
    if (tables_.count(stmt.table_name)) {
        throw std::runtime_error(
        "Table '" + stmt.table_name + "' already exists.");
    }
    std::string path = base_dir_ + "/" + stmt.table_name + ".db";

    TableInfo handle;
    handle.disk_manager = std::make_unique<DiskManager>(path);
    handle.tree = std::make_unique<BPlusTree>(*handle.disk_manager);
    handle.schema.initialized = true;
    handle.schema.table_name = stmt.table_name;
    handle.schema.key_column = stmt.key_column;
    handle.schema.value_column = stmt.value_column;
    handle.schema.value_type = stmt.value_type;
    write_schema(*handle.disk_manager, handle.schema);
    tables_[stmt.table_name] = std::move(handle);
    return "Table '" + stmt.table_name + "' created.";
}
std::string Executor::execute_insert(const InsertStmt& stmt) {
    auto& table = get_table(stmt.table_name);
    table.tree->insert(stmt.key, stmt.value);
    return "OK";
}
std::string Executor::execute_select(const SelectStmt& stmt) {
    auto& table = get_table(stmt.table_name);

    std::string value;
    if (table.tree->search(stmt.search_key, value)) {
        return table.schema.key_column + " | " + table.schema.value_column + "\n" + std::to_string(stmt.search_key) + " | " + value;
    }
    return "No row found with " + table.schema.key_column + " = " + std::to_string(stmt.search_key) + ".";
}
std::string Executor::execute_delete(const DeleteStmt& stmt) {
    auto& table = get_table(stmt.table_name);
if (table.tree->delete_key(stmt.search_key)) {
        return "Deleted " + table.schema.key_column + " =" + std::to_string(stmt.search_key) + ".";
    }

    return "No row found with " + table.schema.key_column + "= " + std::to_string(stmt.search_key) + ".";
}
std::string Executor::execute_select_all(const SelectAllStmt& stmt) {
    auto& table = get_table(stmt.table_name);
    auto rows = table.tree->scan_all();
    if (rows.empty()) {
        return "No rows in table '" + stmt.table_name + "'.";
    }
    std::string result = table.schema.key_column + " | " + table.schema.value_column;
    for (const auto& [k, v] : rows) {result += "\n" + std::to_string(k) + " | " + (v);
    }
    return result;
}

std::string Executor::execute_join_select(const JoinSelectStmt& stmt) {
      auto& left = get_table(stmt.left_table);
      auto& right = get_table(stmt.right_table);

    // figure out which field (key or value) each ON column refers to
    bool left_uses_key = (stmt.left_column == left.schema.key_column);
    bool right_uses_key = (stmt.right_column == right.schema.key_column);

    // scan both tables and match rows
    auto left_rows = left.tree->scan_all();
    auto right_rows = right.tree->scan_all();
    std::string result = left.schema.key_column  + " | " + left.schema.value_column + " | " + right.schema.key_column + " | " + right.schema.value_column;
    // checking every pair
    int match_count = 0;
    for (const auto& [lk, lv] : left_rows) {
        std::string l_val = left_uses_key ? std::to_string(lk) : lv;
        for (const auto& [rk, rv] : right_rows) {
            std::string r_val = right_uses_key ? std::to_string(rk) : rv;
            if (l_val == r_val) {
                result += "\n" + std::to_string(lk) + " | " + lv + " | " + std::to_string(rk) + "" + rv;
                ++match_count;
            }
        }
    }

    if (match_count == 0) {
        return "No matching rows.";
    }
    return result;
}


TableInfo& Executor::get_table(const std::string& name) {
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        throw std::runtime_error("no such table: " + name);
    }
    return it->second;
}
Schema Executor::read_schema(DiskManager& dm) const {
    Schema schema;
    if (dm.num_pages() == 0) {
        return schema;
    }

    Page page{};
    dm.read_page(0, page);

    uint32_t init_flag = 0;
    std::memcpy(&init_flag, page.data() + SCHEMA_INIT_OFFSET, sizeof(uint32_t));
    if (init_flag != 1) {
        return schema;
    }
    schema.initialized = true;
    schema.table_name = std::string(page.data() + TABLE_NAME_OFFSET);
    schema.key_column = std::string(page.data() + KEY_COLUMN_OFFSET);
    schema.value_column = std::string(page.data() + VALUE_COLUMN_OFFSET);
    schema.value_type = std::string(page.data() + VALUE_TYPE_OFFSET);         
    if (schema.value_type.empty()) schema.value_type = "INT"; 
    return schema;
}
// write schema fields to page 0, careful not to overwrite btree's root pointer at bytes 0..3
  void Executor::write_schema(DiskManager& dm, const Schema& schema) {
    Page page{};
    dm.read_page(0, page);
    // zero each field before writing to clear old data
    uint32_t init_flag = schema.initialized ? 1 : 0;
    std::memcpy(page.data() + SCHEMA_INIT_OFFSET, &init_flag, sizeof(uint32_t));
    std::memset(page.data() + TABLE_NAME_OFFSET, 0, IDENT_FIELD_SIZE);
    std::memcpy(page.data() + TABLE_NAME_OFFSET,schema.table_name.c_str(), schema.table_name.size() + 1);
    std::memset(page.data() + KEY_COLUMN_OFFSET, 0, IDENT_FIELD_SIZE);
    std::memcpy(page.data() + KEY_COLUMN_OFFSET,schema.key_column.c_str(), schema.key_column.size() + 1);
    std::memset(page.data() + VALUE_COLUMN_OFFSET, 0, IDENT_FIELD_SIZE);
    std::memcpy(page.data() + VALUE_COLUMN_OFFSET,schema.value_column.c_str(), schema.value_column.size() + 1);
    std::memset(page.data() + VALUE_TYPE_OFFSET, 0, IDENT_FIELD_SIZE);
    std::memcpy(page.data() + VALUE_TYPE_OFFSET, schema.value_type.c_str(),schema.value_type.size() + 1);
    dm.write_page(0, page);
}
} 