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
#include <variant>



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

//Tests for DiskManager
static void test_disk_manager() {

    const std::string test_file = "test_diskmanager.db";
    std::remove (test_file.c_str());

    std::cout << "Disk Manager Tests \n";

    //File creation and the initial state
    std::cout <<"File creation\n";
    stratadb::DiskManager dm(test_file);
    check(dm.num_pages() == 0, "new file starts with 0 pages");
    check(dm.file_path() == test_file, "file_path() returns the correct path");

    //Page allocation
    std::cout << "\nPage allocation";
    stratadb::page_id_t p0 = dm.allocate_page();
    stratadb::page_id_t p1 = dm.allocate_page();
    stratadb::page_id_t p2 = dm.allocate_page();
    check(p0 == 0, "first page has id 0");
    check(p1 == 1, "second page has id 1");
    check(p2 == 2, "third page has id 2");
    check(dm.num_pages() == 3, "num_pages is 3 after 3 allocations");

    //Write and read back
    std::cout << "\nWrite and read\n";
    stratadb::Page write_buf{};
    const char* message = "Hello, StrataDB!";
    std::memcpy(write_buf.data(), message, std::strlen(message));
    dm.write_page(1, write_buf);

    stratadb::Page read_buf{};
    dm.read_page(1, read_buf);
    check(read_buf == write_buf, "read_page returns what was written");
    check(std::memcmp(read_buf.data(), message, std::strlen(message)) == 0, "content matches espected string");

    //Zero-fill verification
    std::cout << "\nZero-fill verification";
    stratadb::Page zero_buf{};
    stratadb::Page alloc_read{};
    dm.read_page(0, alloc_read);
    check(alloc_read == zero_buf, "allocated page 0 is zero-filled");
    dm.read_page(2, alloc_read);
    check(alloc_read == zero_buf, "allocated page 2 is zero-filled");
    
    //Validation
    std::cout <<"\n[Validation\n";
    stratadb::Page dummy{};
    check_throws([&]() {dm.read_page(3, dummy); }, "read_page throws for page_id == num_pages");
    check_throws([&]() {dm.read_page(100, dummy); }, "read_page throws for page_id far out of range");
    check_throws([&]() {dm.write_page(5, dummy); }, "write_page throws for page_id > num_pages");

    //Reopening exist. file 
    std::cout << "\nReopen existing file\n";
    {
        stratadb::DiskManager dm2(test_file);
        check(dm2.num_pages() == 3, "reopened file still has 3 pages");
        
        stratadb::Page reopen_buf{};
        dm2.read_page(1, reopen_buf);
        check(reopen_buf == write_buf, "data is still present fater reopen");
    }

    std::remove(test_file.c_str());

}

//Node tests
static void test_leaf_node() {
    std::cout << "\nLeaf Node tests\n";

    //Inserting keys out of order - i expect for them to end up sorted
    stratadb::LeafNode leaf;
    leaf.insert(30, 300);
    leaf.insert(10, 100);
    leaf.insert(20, 200);

    check(leaf.num_keys() == 3, "Leaf has 3 keys after 3 inserts");
    check(leaf.key_at(0) == 10, "keys sorted: index 0 is 10");
    check(leaf.key_at(1) == 20, "keys sorted: index 1 is 20");
    check(leaf.key_at(2) == 30, "keys sorted: index 2 is 30");

    check(leaf.find_key(10) == 0, "find_key(10) returns 0");
    check(leaf.find_key(20) == 1, "find_key(20) returns 1");
    check(leaf.find_key(30) == 2, "find_key(30) returns 2");
    check(leaf.find_key(99) == -1, "find_key(99) returns -1 (not found)");
    check(leaf.value_at(0) == 100, "value at index 0 is 100");
    check(leaf.find_key(2) == 300, "value at index 2 is 300");

    //Serialization roundtrip (making sure that i can save the code to disk and get the same data back)
    stratadb::Page page{};
    leaf.serialize(page);

    auto desirialized = stratadb::Node::deserialize(page);
    check(desirialized->is_leaf(), "deserialized node is a leaf");
    check(desirialized->num_keys() == 3, "desirialized leaf has 3 keys");

    auto* leaf2 = static_cast<stratadb::LeafNode*>(desirialized.get());
    check(leaf2->key_at(0) == 10, "roundtrip: key 0 is 10");
    check(leaf2->key_at(2) == 30, "roundtrip: key 2 is 30");
    check(leaf2->value_at(1) == 200, "roundtrip: value 1 is 200");
    check(leaf2->find_key(20) == 1, "roundtrip: find_key still works");

}

static void test_internal_node() {
    std::cout << "\nInternalNode tests";


    stratadb::InternalNode internal;
    internal.set_child(0, 100);
    internal.insert_key_child(10, 101);
    internal.insert_key_child(20,102);
    internal.insert_key_child(30, 103);

    check(internal.num_keys() == 3, "internal has 3 keys");
    check(internal.key_at(0) == 10, "key 0 is 10");
    check(internal.key_at(1) == 20, "key 1 is 20");
    check(internal.child_at(0) == 100, "child 0 is 100");
    check(internal.child_at(3) == 103, "child 3 is 103");

    check(internal.find_child_index(5) == 0, "key 5 -> child 0");
    check(internal.find_child_index(15) == 1, "key 15 -> child 1");
    check(internal.find_child_index(25) == 2, "key 25 -> child 2");
    check(internal.find_child_index(35) == 3, "key 35 -> child 3");

    stratadb::Page page{};
    internal.serialize(page);

    auto deserialized = stratadb::Node::deserialize(page);
    check(!deserialized->is_leaf(), "deserialized node is internal");
    check(deserialized->num_keys() == 3, "deserialized internal has 3 keys");

    auto* int2 = static_cast<stratadb::InternalNode*>(deserialized.get());
    check(int2->child_at(0) == 100, "roundtrip: child 0 is 100");
    check(int2->child_at(2) == 102, "roundtrip: child 2 is 102");
    check(int2->find_child_index(15) == 1, "roundtrip: find_child_index works");


}

//B+ Tree tests

static void test_btree_empty() {
    const std::string test_file = "test_btree.db";
    std::remove(test_file.c_str());

    std::cout<<"\nB+Tree: empty tree";
    stratadb::DiskManager dm(test_file);
    stratadb::BPlusTree tree(dm);

    check(tree.is_empty(), "new tree is empty");
    check(tree.root_page_id() == 0, "empty tree has root_page_id = 0 (sentinel)");

    int32_t val = -1;
    check(!tree.search(42, val), "search in empty tree returns false");

    // Metadata page should exist.
    check(dm.num_pages() == 1, "metadata page allocated");

     std::remove(test_file.c_str());

}

static void test_btree_first_insert() {
    const std::string test_file = "test_btree.db";
    std::remove(test_file.c_str());

    std::cout << "\nTest: for first insert";
    stratadb::DiskManager dm(test_file);
    stratadb::BPlusTree tree(dm);

    tree.insert(10, 100);
    check(!tree.is_empty(), "tree not empty after insert");
    check(tree.root_page_id() != 0, "root page changed from sentinel");

    int32_t val = -1;
    check(tree.search(10, val), "search finds key 10");
    check(val == 100, "value for key 10 is 100");
    check(!tree.search(99, val), "non-existent key 99 not found");

    std::remove(test_file.c_str());
}

static void test_btree_multi_insert() {
      const std::string test_file = "test_btree.db";
      std::remove(test_file.c_str());

      std::cout << "\n[Test: multiple inserts no split]\n";
      stratadb::DiskManager dm(test_file);
      stratadb::BPlusTree tree(dm);

      tree.insert(30, 300);
      tree.insert(10, 100);
      tree.insert(40, 400);
      tree.insert(20, 200);

      int32_t val = -1;
      check(tree.search(10, val) && val == 100, "key 10 → 100");
      check(tree.search(20, val) && val == 200, "key 20 → 200");
      check(tree.search(30, val) && val == 300, "key 30 → 300");
      check(tree.search(40, val) && val == 400, "key 40 → 400");

      std::remove(test_file.c_str());
  }

  static void test_btree_many_keys() {
      const std::string test_file = "test_btree.db";
      std::remove(test_file.c_str());

      std::cout << "\n[Test: 25 sequential keys]\n";
      stratadb::DiskManager dm(test_file);
      stratadb::BPlusTree tree(dm);

      for (int i = 1; i <= 25; ++i) {
          tree.insert(i, i * 10);
      }

      bool all_found = true;
      for (int i = 1; i <= 25; ++i) {
          int32_t val;
          if (!tree.search(i, val) || val != i * 10) {
              std::cout << "    MISSING key " << i << "\n";
              all_found = false;
          }
      }
      check(all_found, "all 25 sequential keys found");
      std::cout << "    (tree uses " << dm.num_pages() << " pages)\n";

      std::remove(test_file.c_str());
  }

  static void test_btree_reverse() {
      const std::string test_file = "test_btree.db";
      std::remove(test_file.c_str());

      std::cout << "\n[Test: reverse-order inserts]\n";
      stratadb::DiskManager dm(test_file);
      stratadb::BPlusTree tree(dm);

      for (int i = 20; i >= 1; --i) {
          tree.insert(i, i * 100);
      }

      bool all_found = true;
      for (int i = 1; i <= 20; ++i) {
          int32_t val;
          if (!tree.search(i, val) || val != i * 100) all_found = false;
      }
      check(all_found, "all 20 reverse-inserted keys found");

      std::remove(test_file.c_str());
  }

  static void test_btree_random_order() {
      const std::string test_file = "test_btree.db";
      std::remove(test_file.c_str());

      std::cout << "\n[Test: random-order inserts]\n";
      stratadb::DiskManager dm(test_file);
      stratadb::BPlusTree tree(dm);

      std::vector<int> keys = {15, 3, 22, 8, 19, 1, 30, 12, 25, 6,
                               28, 17, 10, 27, 5, 20, 14, 2, 24, 9};
      for (int k : keys) {
          tree.insert(k, k * 11);
      }

      bool all_found = true;
      for (int k : keys) {
          int32_t val;
          if (!tree.search(k, val) || val != k * 11) all_found = false;
      }
      check(all_found, "all 20 randomly-ordered keys found");

      std::remove(test_file.c_str());
  }

static void test_btree_delete() {
    const std::string f = "test_del.db";
    std::remove(f.c_str());                                                                       
    stratadb::DiskManager dm(f);
    stratadb::BPlusTree tree(dm);                                                                 
                  
    tree.insert(10, 100);
    tree.insert(20, 200);

    int32_t val = -1;
    check(tree.delete_key(10), "delete_key works");
    check(!tree.search(10, val), "deleted key gone");

    std::remove(f.c_str());
}

static void test_e2e_delete() {
    const std::string f = "test_del2.db";
    std::remove(f.c_str());
    stratadb::DiskManager dm(f);
    stratadb::Executor exec(dm);

    run_sql(exec, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    run_sql(exec, "INSERT INTO t VALUES (1, 100)");
    check(run_sql(exec, "DELETE FROM t WHERE id = 1") == "Deleted row with id = 1.","DELETE via SQL works");

    std::remove(f.c_str());
}

static void test_parser() {                                                                       
    std::cout << "\n Parser Tests\n";
                                                                                                    
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
        check(insert.value == 95, "INSERT parses value");
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

    {
        auto stmt = stratadb::Parser::parse("INSERT INTO t VALUES (-5, -100)");
        auto& insert = std::get<stratadb::InsertStmt>(stmt);
        check(insert.key == -5, "negative key works");
    }

      // It is intended t oreject bad sql
    check_throws([]() { stratadb::Parser::parse("HELLO WORLD"); },"rejects unknown statement");
check_throws([]() { stratadb::Parser::parse("SELECT * FROM"); },"rejects incomplete SQL");
  }

static std::string run_sql(stratadb::Executor& exec, const std::string& sql) {                 
      auto stmt = stratadb::Parser::parse(sql);
      return exec.execute(stmt);                                                                     
}                                                                                                

static void test_executor() {                             
    std::cout << "\n=== Executor Tests ===\n";

    std::remove("data/users.db");
    stratadb::Executor executor("data");

    std::cout << executor.execute(stratadb::Parser::parse("CREATE TABLE users (id INT PRIMARY KEY, age INT)")) << "\n";
    std::cout << executor.execute(stratadb::Parser::parse("INSERT INTO users VALUES (1, 25)")) << "\n";
    std::cout << executor.execute(stratadb::Parser::parse("SELECT * FROM users WHERE id = 1")) << "\n";

    std::cout << "Executor tests passed.\n";
}




int main() {

    test_disk_manager();

    std::cout <<"Node tests";
    test_leaf_node();
    test_internal_node();
    test_btree_multi_insert();

    std::cout << "B+ Tree Tests";
      test_btree_empty();
      test_btree_first_insert();
      test_btree_delete();
      test_e2e_delete();

    std::cout <<"Parser Tests";
    test_parser();
    test_executor();


    std::cout << "\nResults: " << test_passed << " passed, " << test_failed << " failed\n";
    
    return test_failed > 0 ? 1 : 0;

}