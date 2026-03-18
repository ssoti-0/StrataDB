#include "storage/disk_manager.h"
#include "index/node.h"
#include "index/btree.h"

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>


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

  static void test_btree_leaf_split() {
      const std::string test_file = "test_btree.db";
      std::remove(test_file.c_str());

      std::cout << "\n[Test: leaf split]\n";
      stratadb::DiskManager dm(test_file);
      stratadb::BPlusTree tree(dm);

      tree.insert(10, 100);
      tree.insert(20, 200);
      tree.insert(30, 300);
      tree.insert(40, 400);
      tree.insert(50, 500);  // 5th key — triggers split

      int32_t val;
      check(tree.search(10, val) && val == 100, "key 10 after split");
      check(tree.search(20, val) && val == 200, "key 20 after split");
      check(tree.search(30, val) && val == 300, "key 30 after split");
      check(tree.search(40, val) && val == 400, "key 40 after split");
      check(tree.search(50, val) && val == 500, "key 50 after split");

      std::remove(test_file.c_str());
  }

//Add duplicate key test - maybe later on

int main() {

    test_disk_manager();

    std::cout <<"Node tests";
    test_leaf_node();
    test_internal_node();
    test_btree_multi_insert();

    std::cout << "B+ Tree Tests";
      test_btree_empty();
      test_btree_first_insert();

    std::cout << "\nResults: " << test_passed << " passed, " << test_failed << " failed\n";
    
    return test_failed > 0 ? 1 : 0;

}