#include "execution/executor.h"
#include <chrono>
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
      if (std::get_if<StatsStmt>(&stmt))
          return execute_stats();
      if (auto* s = std::get_if<VerboseStmt>(&stmt))
          return execute_verbose(*s);
      if (std::get_if<BenchmarkStmt>(&stmt))
          return execute_benchmark();
      throw std::runtime_error("unknown statement type");
 }

std::string Executor::execute_create(const CreateTableStmt& stmt) {
    std::string out;
    vlog(out, "[Parser]    Parsed CREATE TABLE statement");
    vlog(out, "[Executor]  Creating table '" + stmt.table_name + "' (" + stmt.key_column + " INT PK, " + stmt.value_column + " " + stmt.value_type + ")");

    if (tables_.count(stmt.table_name)) {
        throw std::runtime_error("Table '" + stmt.table_name + "' already exists.");
    }
    std::string path = base_dir_ + "/" + stmt.table_name + ".db";

    TableInfo handle;
    handle.disk_manager = std::make_unique<DiskManager>(path);
    vlog(out, "[Storage]   Created file '" + path + "'");
    handle.tree = std::make_unique<BPlusTree>(*handle.disk_manager);
    vlog(out, "[B+ Tree]   Initialized empty tree (metadata page 0 allocated)");
    handle.schema.initialized = true;
    handle.schema.table_name = stmt.table_name;
    handle.schema.key_column = stmt.key_column;
    handle.schema.value_column = stmt.value_column;
    handle.schema.value_type = stmt.value_type;
    write_schema(*handle.disk_manager, handle.schema);
    vlog(out, "[Storage]   Schema written to page 0");
    tables_[stmt.table_name] = std::move(handle);
    out += "Table '" + stmt.table_name + "' created.";
    return out;
}
std::string Executor::execute_insert(const InsertStmt& stmt) {
    std::string out;
    vlog(out, "[Parser]Parsed INSERT statement");
    auto& table = get_table(stmt.table_name);

    for (const auto& row : stmt.rows) {
        vlog(out, "[Executor]Inserting key=" + std::to_string(row.first) + " value='" + row.second + "' into '" + stmt.table_name + "'");
        vlog(out, "[B+ Tree]Navigating from root (page " + std::to_string(table.tree->root_page_id()) + ") to target leaf");
        table.tree->insert(row.first, row.second);
    }
    vlog(out, "[Storage]Write completed (" + std::to_string(table.disk_manager->num_pages()) + " pages total)");
    out += "OK (" + std::to_string(stmt.rows.size()) + " row" + (stmt.rows.size() > 1 ? "s" : "") + ")";    
    return out;
}
std::string Executor::execute_select(const SelectStmt& stmt) {
    std::string out;
    vlog(out, "[Parser] Parsed SELECT statement");
    vlog(out, "[Executor] Searching key=" + std::to_string(stmt.search_key) + " in '" + stmt.table_name + "'");
    auto& table = get_table(stmt.table_name);
    vlog(out, "[B+ Tree]   Traversing from root (page " + std::to_string(table.tree->root_page_id()) + ") to leaf");
    std::string value;
    if (table.tree->search(stmt.search_key, value)) {
        vlog(out, "[B+ Tree]   Key found in leaf node");
        out += table.schema.key_column + " | " + table.schema.value_column + "\n" +
    std::to_string(stmt.search_key) + " | " + value;
        return out;
    }
    vlog(out, "[B+ Tree]   Key not found");
    out += "No row found with " + table.schema.key_column + " = " +
    std::to_string(stmt.search_key) + ".";
    return out;
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

std::string Executor::execute_delete(const DeleteStmt& stmt) {
    std::string out;
    vlog(out, "[Parser] Parsed DELETE statement");
    vlog(out, "[Executor] Deleting key=" + std::to_string(stmt.search_key) + " from '" + stmt.table_name + "'");
    auto& table = get_table(stmt.table_name);
    vlog(out, "[B+ Tree] Navigating to leaf containing key");
    if (table.tree->delete_key(stmt.search_key)) {
        vlog(out, "[B+ Tree]Key removed (rebalancing applied if needed)");
        out += "Deleted " + table.schema.key_column + " =" + std::to_string(stmt.search_key) + ".";
        return out;
    }
    vlog(out, "[B+ Tree]   Key not found — nothing to delete");
    out += "No row found with " + table.schema.key_column + "= " + std::to_string(stmt.search_key) + ".";
    return out;
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
                result += "\n" + std::to_string(lk) + " | " + lv + " | " + std::to_string(rk) + " | " + rv;
                ++match_count;
            }
        }
    }

    if (match_count == 0) {
        return "No matching rows.";
    }
    return result;
}

std::string Executor::execute_stats() {
    std::string out = "StrataDB Statistics\n";
    out += "\nLoaded tables: " + std::to_string(tables_.size()) + "\n";

    uint64_t total_reads = 0, total_writes = 0;
    uint64_t total_splits = 0, total_merges = 0, total_redist = 0;

    for (auto& [name, info] : tables_) {
        out += "\nTable: " + name + "\n";
        auto rows = info.tree->scan_all();
        out += "Records: " + std::to_string(rows.size()) + "\n";
        out += "Pages on disk: " + std::to_string(info.disk_manager->num_pages()) + "\n";
        out += "Page reads: " + std::to_string(info.disk_manager->read_count()) + "\n";
        out += "Page writes: " + std::to_string(info.disk_manager->write_count()) + "\n";
        out += "Node splits: " + std::to_string(info.tree->split_count()) + "\n";
        out += "Node merges: " + std::to_string(info.tree->merge_count()) + "\n";
        out += "Redistributions: " + std::to_string(info.tree->redistribute_count()) + "\n";

        total_reads += info.disk_manager->read_count();
        total_writes += info.disk_manager->write_count();
        total_splits += info.tree->split_count();
        total_merges += info.tree->merge_count();
        total_redist += info.tree->redistribute_count();
    }

      if (tables_.size() > 1) {
        out += "\nTotals\n";
        out += "Total page reads: " + std::to_string(total_reads) + "\n";
        out += "Total page writes: " + std::to_string(total_writes) + "\n";
        out += "Total splits: " + std::to_string(total_splits) + "\n";
        out += "Total merges: " + std::to_string(total_merges) + "\n";
        out += "Total redistributions: " + std::to_string(total_redist);
    }

    return out;
}

std::string Executor::execute_verbose(const VerboseStmt& stmt) {
    verbose_ = stmt.enable;
    return std::string("Verbose mode ") + (verbose_ ? "ON" : "OFF") + ".";
}

std::string Executor::execute_benchmark() {
    using Clock = std::chrono::high_resolution_clock;
    std::string out = "StrataDB Benchmark\n";
    const int scales[] = {25, 100, 1000};

    for (int n : scales) {
        // create a temporary database file for this run
        std::string tmp_file = base_dir_ + "/_benchmark_tmp.db";
        std::remove(tmp_file.c_str());
        DiskManager dm(tmp_file);
        BPlusTree tree(dm);

        //INSERT benchmark 
        auto t0 = Clock::now();
        for (int i = 1; i <= n; i++) {
            tree.insert(i, std::to_string(i * 10));
        }
        auto t1 = Clock::now();
        auto insert_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

        //SEARCH benchmark 
        t0 = Clock::now();
        std::string val;
        for (int i = 1; i <= n; i++) {
            tree.search(i, val);
        }
        t1 = Clock::now();
        auto search_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

        //SCAN benchmark 
        t0 = Clock::now();
        auto rows = tree.scan_all();
        t1 = Clock::now();
        auto scan_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

        //DELETE benchmark
        t0 = Clock::now();
        for (int i = 1; i <= n; i++) {
            tree.delete_key(i);
        }
        t1 = Clock::now();
        auto delete_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
        out += "\n" + std::to_string(n) + " keys:\n";
        out += "Insert: " + std::to_string(insert_us) + " us\n";
        out += "Search: " + std::to_string(search_us) + " us\n";
        out += "Scan: " + std::to_string(scan_us) + " us\n";
        out += "Delete: " + std::to_string(delete_us) + " us\n";
        out += "Pages: " + std::to_string(dm.num_pages()) + "\n";
        std::remove(tmp_file.c_str());
    }
    return out;
}
void Executor::vlog(std::string& out, const std::string& msg) const {
    if (verbose_) {
        out += msg + "\n";
    }
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