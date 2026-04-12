#include "storage/disk_manager.h"
#include "index/node.h"
#include "index/btree.h"
#include "query/parser.h"
#include "execution/executor.h"

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

static int test_passed = 0;
static int test_failed = 0;

static void check(bool condition, const std::string& description) {
    if (condition) {
        std::cout << " PASS: " << description << "\n";
        ++test_passed;
    } else {
        std::cout << "FAIL: " << description << "\n";
        ++test_failed;
    }
}

template <typename Func>
static void check_throws(Func&& func, const std::string& description) {
    try {
        func();
        std::cout << "FAIL: " << description << " (no exception thrrown)\n";
        ++test_failed;
    } catch (const std::runtime_error&) {
        std::cout << "PASS: " << description << "\n";
        ++test_passed;
    }
}

static std::string run_sql(stratadb::Executor& exec, const std::string& sql) {
      auto stmt = stratadb::Parser::parse(sql);
      return exec.execute(stmt);
  }


// basic file and page operations  
static void test_disk_manager() {

    const std::string test_file = "test_diskmanager.db";
    std::remove (test_file.c_str());

    std::cout << "Disk Manager Tests \n";
    stratadb::DiskManager dm(test_file);
    check(dm.num_pages() == 0, "new file starts with 0 pages");
    check(dm.file_path() == test_file, "file_path() returns the correct path");
    stratadb::page_id_t p0 = dm.allocate_page();
    stratadb::page_id_t p1 = dm.allocate_page();
    stratadb::page_id_t p2 = dm.allocate_page();
    check(p0 == 0, "first page has id 0");
    check(p1 == 1, "second page has id 1");
    check(p2 == 2, "third page has id 2");
    check(dm.num_pages() == 3, "num_pages is 3 after 3 allocations");
    stratadb::Page write_buf{};
    const char* message = "Hello, StrataDB!";
    std::memcpy(write_buf.data(), message, std::strlen(message));
    dm.write_page(1, write_buf);
    stratadb::Page read_buf{};
    dm.read_page(1, read_buf);
    check(read_buf == write_buf, "read_page returns what was written");
    check(std::memcmp(read_buf.data(), message, std::strlen(message)) == 0, "content matches espected string");
    stratadb::Page zero_buf{};
    stratadb::Page alloc_read{};
    dm.read_page(0, alloc_read);
    check(alloc_read == zero_buf, "allocated page 0 is zero-filled");
    stratadb::Page dummy{};
    check_throws([&]() {dm.read_page(3, dummy); }, "read_page throws for page_id == num_pages");
    check_throws([&]() {dm.write_page(5, dummy); }, "write_page throws for page_id > num_pages");
    std::remove(test_file.c_str());

}

// make sure nodes store keys in order and survive serialization
static void test_nodes() {
    std::cout << "\n Node tests\n";

    stratadb::LeafNode leaf;
    leaf.insert(30, "300");
    leaf.insert(10, "100");
    leaf.insert(20, "200");

    check(leaf.num_keys() == 3,"Leaf has 3 keys after 3 inserts");
    check(leaf.key_at(0) == 10,"keys sorted: index 0 is 10");
    check(leaf.key_at(2) == 30,"keys sorted: index 2 is 30");
    check(leaf.find_key(20) == 1, "find_key(20) returns 1");
    check(leaf.find_key(99) == -1, "find_key(99) returns -1 (not found)");
    check(leaf.value_at(2) == "300", "value at index 2 is 300");

    // serialize and read back to make sure nothing gets lost
    stratadb::Page page{};
    leaf.serialize(page);
    auto desirialized = stratadb::Node::deserialize(page);
    check(desirialized->is_leaf(), "deserialized node is a leaf");
    check(desirialized->num_keys() == 3, "desirialized leaf has 3 keys");

    auto* leaf2 = static_cast<stratadb::LeafNode*>(desirialized.get());
    check(leaf2->key_at(0) == 10, "roundtrip: key 0 is 10");
    check(leaf2->value_at(1) == "200", "roundtrip: value 1 is 200");

    stratadb::InternalNode internal;
    internal.set_child(0, 100);
    internal.insert_key_child(10, 101);
    internal.insert_key_child(20,102);

      check(internal.find_child_index(5) == 0, "key 5 -> child 0");
    check(internal.find_child_index(15) == 1, "key 15 -> child 1");
    check(internal.find_child_index(25) == 2, "key 25 -> child 2");

}

// insert and search without triggering any splits
static void test_btree() {
 const std::string f = "test_btree.db";
    std::remove(f.c_str());

    std::cout << "\nB+ Tree tests\n";
    stratadb::DiskManager dm(f);
    stratadb::BPlusTree tree(dm);

    check(tree.is_empty(), "new tree is empty");

    std::string val;
    check(!tree.search(42, val), "search in empty tree returns false");

    tree.insert(10, "100");
    tree.insert(30, "300");
    tree.insert(20, "200");
    tree.insert(40, "400");

    check(tree.search(10, val) && val == "100", "key 10 found");
    check(tree.search(30, val) && val == "300", "key 30 found");
    check(!tree.search(99, val), "key 99 not found");

    std::remove(f.c_str());

}
// 25 sequential inserts — forces leaf and internal node splits
static void test_btree_splits() {
    const std::string f = "test_splits.db";
    std::remove(f.c_str());

    std::cout << "\nB+ Tree splits (25 keys)\n";
    stratadb::DiskManager dm(f);
    stratadb::BPlusTree tree(dm);

    for (int i = 1; i <= 25; ++i) {
        tree.insert(i, std::to_string(i * 10));
    }
    bool all_found = true;
    for (int i = 1; i <= 25; ++i) {
        std::string val;
        if (!tree.search(i, val) || val != std::to_string(i * 10)) {
            std::cout << "    MISSING key " << i << "\n";
            all_found = false;
        }
    }
    check(all_found, "all 25 keys found after splits");
    std::cout << "    tree uses " << dm.num_pages() << " pages\n";

    std::remove(f.c_str());
}
// delete a key, check its gone and the other one isnt
static void test_btree_delete() {
    const std::string f = "test_del.db";
    std::remove(f.c_str());

    std::cout << "\nB+ Tree delete\n";
    stratadb::DiskManager dm(f);
    stratadb::BPlusTree tree(dm);

    tree.insert(10, "100");
    tree.insert(20, "200");

    std::string val;
    check(tree.delete_key(10), "delete_key returns true");
    check(!tree.search(10, val), "deleted key is gone");
    check(tree.search(20, val) && val == "200", "other key still there");

    std::remove(f.c_str());
}
// parse each statement type and make sure fields are correct
static void test_parser() {
    std::cout << "\nParser Tests\n";
    {
        auto stmt = stratadb::Parser::parse("CREATE TABLE students (id INT PRIMARY KEY, grade INT)");
        auto& create = std::get<stratadb::CreateTableStmt>(stmt);
        check(create.table_name == "students", "CREATE parses table name");
        check(create.key_column == "id", "CREATE parses key column");
    }
    {
        auto stmt = stratadb::Parser::parse("INSERT INTO students VALUES (42, 95)");
        auto& insert = std::get<stratadb::InsertStmt>(stmt);
        check(insert.key == 42, "INSERT parses key");
        check(insert.value == "95", "INSERT parses value");
    }
    {
        auto stmt = stratadb::Parser::parse("SELECT * FROM students WHERE id = 42");
        auto& select = std::get<stratadb::SelectStmt>(stmt);
        check(select.search_key == 42, "SELECT parses search key");
    }
    {
        auto stmt = stratadb::Parser::parse("DELETE FROM students WHERE id = 42");
        auto& del = std::get<stratadb::DeleteStmt>(stmt);
        check(del.search_key == 42, "DELETE parses search key");
    }
    {
        auto stmt = stratadb::Parser::parse("create table T (x int primary key, y int)");
        auto& create = std::get<stratadb::CreateTableStmt>(stmt);
        check(create.table_name == "T", "lowercase keywords work");
    }
    check_throws([]() { stratadb::Parser::parse("HELLO WORLD"); },"rejects unknown statement");
    check_throws([]() { stratadb::Parser::parse("SELECT * FROM"); },"rejects incomplete sql");
}                                                                
// full pipeline: sql string -> parse -> execute -> check result
 static void test_executor() {
    std::cout << "\nExecutor Tests\n";
    std::remove("testdata/users.db");
    stratadb::Executor exec("testdata");

    auto r1 = run_sql(exec, "CREATE TABLE users (id INT PRIMARY KEY, age INT)");
    check(r1 == "Table 'users' created.", "CREATE TABLE works");
    run_sql(exec, "INSERT INTO users VALUES (1, 25)");
    run_sql(exec, "INSERT INTO users VALUES (2, 30)");
    auto sel = run_sql(exec, "SELECT * FROM users WHERE id = 1");
    check(sel.find("25") != std::string::npos, "SELECT finds value");
    auto del = run_sql(exec, "DELETE FROM users WHERE id = 1");
    check(del.find("Deleted") != std::string::npos, "delete ok");
    // make sure the row is actually gone after delete
    auto gone = run_sql(exec, "SELECT * FROM users WHERE id = 1");
    check(gone.find("No row") != std::string::npos, "deleted row gone");

    std::remove("testdata/users.db");
}

static void test_join() {
    std::cout << "\nJoin test\n";

    std::remove("testdata/students.db");
    std::remove("testdata/grades.db");
    stratadb::Executor exec("testdata");

    run_sql(exec,"CREATE TABLE students (id INT PRIMARY KEY, name INT)");
    run_sql(exec,"CREATE TABLE grades (sid INT PRIMARY KEY, score INT)");
    run_sql(exec,"INSERT INTO students VALUES (1, 10)");
    run_sql(exec,"INSERT INTO students VALUES (2, 20)");
    run_sql(exec,"INSERT INTO grades VALUES (1, 95)");
    run_sql(exec,"INSERT INTO grades VALUES (3, 80)");

    auto result = run_sql(exec,"SELECT * FROM students JOIN grades ON students.id = grades.sid");
    check(result.find("95") != std::string::npos, "join found matching row");
}
int main() {
    test_disk_manager();
    test_nodes();
    test_btree();
    test_btree_splits();
    test_btree_delete();
    test_parser();
    test_executor();
    test_join();

    std::cout << "\nResults: " << test_passed << " passed, "<< test_failed << " failed\n";
    return test_failed > 0 ? 1 : 0;
}