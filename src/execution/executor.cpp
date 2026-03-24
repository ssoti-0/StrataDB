#include "execution/executor.h"

  #include <cstring>
  #include <stdexcept>
  #include <variant>

namespace stratadb {

static constexpr std::size_t SCHEMA_INIT_OFFSET  = 4;
static constexpr std::size_t TABLE_NAME_OFFSET   = 8;
static constexpr std::size_t KEY_COLUMN_OFFSET   = 72;
static constexpr std::size_t VALUE_COLUMN_OFFSET = 136;
static constexpr std::size_t IDENT_FIELD_SIZE    = 64;

Executor::Executor(DiskManager& disk_manager) : disk_manager_(disk_manager), tree_(disk_manager) {}

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
    }
}, stmt);
}

std::string Executor::execute_create(const CreateTableStmt& stmt) {
    if (schema_.initialized) {
        throw std::runtime_error("Table '" + schema_.table_name + "' already exists. " + "Only one table per database file is supported.");
    }

    if (stmt.table_name.size() > MAX_IDENTIFIER_LENGTH) {
        throw std::runtime_error("Table name '" + stmt.table_name + "' exceeds maximum length of " + std::to_string(MAX_IDENTIFIER_LENGTH) + " characters.");
    }

    schema_.initialized = true;
    schema_.table_name = stmt.table_name;
    schema_.key_column = stmt.key_column;
    schema_.value_column = stmt.value_column;
    write_schema(schema_);

    return "Table '" + stmt.table_name + "' created.";
}

std::string Executor::execute_insert(const InsertStmt& stmt) {
    require_table(stmt.table_name);
    tree_.insert(stmt.key, stmt.value);
    return "OK";
}

std::string Executor::execute_select(const SelectStmt& stmt) {
    require_table(stmt.table_name);

    int32_t value = 0;
    if (tree_.search(stmt.search_key, value)) {
        return schema_.key_column + " | " + schema_.value_column + "\n" + std::to_string(stmt.search_key) + " | " + std::to_string(value);
    }

    return "No row found with " + schema_.key_column + " = " + std::to_string(stmt.search_key) + ".";
}

std::string Executor::execute_delete(const DeleteStmt& stmt) {
    require_table(stmt.table_name);
    throw std::runtime_error("DELETE not implemented yet");
}

Schema Executor::read_schema() const {
    Schema schema;

    if (disk_manager_.num_pages() == 0) {
        return schema;
    }

    Page page{};
    disk_manager_.read_page(METADATA_PAGE_ID, page);

    uint32_t init_flag = 0;
    std::memcpy(&init_flag, page.data() + SCHEMA_INIT_OFFSET,sizeof(uint32_t));

    if (init_flag != 1) {
        return schema;
    }

    schema.initialized = true;
    schema.table_name = std::string(page.data() + TABLE_NAME_OFFSET);
    schema.key_column = std::string(page.data() + KEY_COLUMN_OFFSET);
    schema.value_column = std::string(page.data() + VALUE_COLUMN_OFFSET);

    return schema;
}

void Executor::write_schema(const Schema& schema) {
    Page page{};

    uint32_t init_flag = schema.initialized ? 1 : 0;
    std::memcpy(page.data() + SCHEMA_INIT_OFFSET, &init_flag,sizeof(uint32_t));

    std::memset(page.data() + TABLE_NAME_OFFSET, 0, IDENT_FIELD_SIZE);
    std::memcpy(page.data() + TABLE_NAME_OFFSET,schema.table_name.c_str(),schema.table_name.size() + 1);

    std::memset(page.data() + KEY_COLUMN_OFFSET, 0, IDENT_FIELD_SIZE);
    std::memcpy(page.data() + KEY_COLUMN_OFFSET,schema.key_column.c_str(),schema.key_column.size() + 1);

    std::memset(page.data() + VALUE_COLUMN_OFFSET, 0, IDENT_FIELD_SIZE);
    std::memcpy(page.data() + VALUE_COLUMN_OFFSET,schema.value_column.c_str(),schema.value_column.size() + 1);

    disk_manager_.write_page(METADATA_PAGE_ID, page);
}

void Executor::require_table(const std::string& table_name) const {
    if (!schema_.initialized) {
        throw std::runtime_error(
        "No table exists. Use CREATE TABLE first.");
    }
    if (table_name != schema_.table_name) {
        throw std::runtime_error(
        "Table '" + table_name + "' does not exist. "
        "The active table is '" + schema_.table_name + "'.");
    }
}

}
