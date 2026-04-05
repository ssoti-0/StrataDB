#include "execution/executor.h"

#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <variant>

namespace stratadb {
static constexpr std::size_t SCHEMA_INIT_OFFSET  = 4;
static constexpr std::size_t TABLE_NAME_OFFSET   = 8;
static constexpr std::size_t KEY_COLUMN_OFFSET   = 72;
static constexpr std::size_t VALUE_COLUMN_OFFSET = 136;
static constexpr std::size_t IDENT_FIELD_SIZE    = 64;

Executor::Executor(const std::string& base_dir) : base_dir_(base_dir) {
    namespace fs = std::filesystem;
    fs::create_directories(base_dir);

    for (const auto& entry : fs::directory_iterator(base_dir)) {
        if (entry.path().extension() != ".db") continue;

        std::string table_name = entry.path().stem().string();
        TableHandle handle;
        handle.disk_manager = std::make_unique<DiskManager>(entry.path().string());
        handle.tree = std::make_unique<BPlusTree>(*handle.disk_manager);
        handle.schema = read_schema(*handle.disk_manager);

        if (handle.schema.initialized) {
            tables_[table_name] = std::move(handle);
        }
    }
}


std::string Executor::execute(const Statement& stmt) {
    return std::visit([this](const auto& s) -> std::string {
        using T = std::decay_t<decltype(s)>;
        if constexpr (std::is_same_v<T, CreateTableStmt>) {
            return execute_create(s);
        } else if constexpr (std::is_same_v<T, InsertStmt>) {
            return execute_insert(s);
        } else if constexpr (std::is_same_v<T, SelectStmt>) {
            return execute_select(s);
        } else if constexpr (std::is_same_v<T, DeleteStmt>) {
            return execute_delete(s);
        } else if constexpr (std::is_same_v<T, JoinSelectStmt>) {
            return execute_join_select(s);
        }
    }, stmt);
}


std::string Executor::execute_create(const CreateTableStmt& stmt) {
    if (tables_.count(stmt.table_name)) {
        throw std::runtime_error(
        "Table '" + stmt.table_name + "' already exists.");
    }
    std::string path = base_dir_ + "/" + stmt.table_name + ".db";

    TableHandle handle;
    handle.disk_manager = std::make_unique<DiskManager>(path);
    handle.tree = std::make_unique<BPlusTree>(*handle.disk_manager);
    handle.schema.initialized = true;
    handle.schema.table_name = stmt.table_name;
    handle.schema.key_column = stmt.key_column;
    handle.schema.value_column = stmt.value_column;
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

    int32_t value = 0;
    if (table.tree->search(stmt.search_key, value)) {
        return stmt.table_name + " | " + table.schema.value_column + "\n" + std::to_string(stmt.search_key) + " | " +
            std::to_string(value);
    }
    return "No row found with " + table.schema.key_column + " = " + std::to_string(stmt.search_key) + ".";
}
std::string Executor::execute_delete(const DeleteStmt& stmt) {
    auto& table = get_table(stmt.table_name);
if (table.tree->delete_key(stmt.search_key)) {
        return "Deleted row with " + table.schema.key_column + " = " + std::to_string(stmt.search_key) + ".";
    }

    return "No row found with " + table.schema.key_column + " = " + std::to_string(stmt.search_key) + ".";
}
std::string Executor::execute_join_select(const JoinSelectStmt& stmt) {
    return "JOIN not implemented yet.";
}
TableHandle& Executor::get_table(const std::string& name) {
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        throw std::runtime_error("Table '" + name + "' does not exist. Use CREATE TABLE first.");
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
    schema.table_name   = std::string(page.data() + TABLE_NAME_OFFSET);
    schema.key_column   = std::string(page.data() + KEY_COLUMN_OFFSET);
    schema.value_column = std::string(page.data() + VALUE_COLUMN_OFFSET);
    return schema;
}

  void Executor::write_schema(DiskManager& dm, const Schema& schema) {
    Page page{};
    dm.read_page(0, page);

    uint32_t init_flag = schema.initialized ? 1 : 0;
    std::memcpy(page.data() + SCHEMA_INIT_OFFSET, &init_flag, sizeof(uint32_t));
    std::memset(page.data() + TABLE_NAME_OFFSET, 0, IDENT_FIELD_SIZE);
    std::memcpy(page.data() + TABLE_NAME_OFFSET,schema.table_name.c_str(), schema.table_name.size() + 1);
    std::memset(page.data() + KEY_COLUMN_OFFSET, 0, IDENT_FIELD_SIZE);
    std::memcpy(page.data() + KEY_COLUMN_OFFSET,schema.key_column.c_str(), schema.key_column.size() + 1);
    std::memset(page.data() + VALUE_COLUMN_OFFSET, 0, IDENT_FIELD_SIZE);
    std::memcpy(page.data() + VALUE_COLUMN_OFFSET,schema.value_column.c_str(), schema.value_column.size() + 1);

    dm.write_page(0, page);
}
} 