#include "storage/disk_manager.h"
#include "index/node.h"

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

}
int main() {

    test_disk_manager();

    std::cout << "\nResults: " << test_passed << " passed, " << test_failed << " failed\n";
    
    return test_failed > 0 ? 1 : 0;

}