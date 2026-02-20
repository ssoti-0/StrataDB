#include "storage/disk_manager.h"

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
        ++tests_passed;
    }
}

static void test_disk_manager() {

    const std::string test_file = "test_diskmanager.db";
    std::remove (test_file.c_str());

    std::cout << "Disk Manager Tests \n";

    //File creation and the initial state
    std::cout <<"File creation\n";
    stratadb::DiskManager dm(test_file);
    check(dm.num_pages() == 0, "new file starts with 0 pages");
    check(dm.file_path() == test_file, "file_path() returns the correct path");

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

    //Page allocation
    std::cout << "\nPage allocation;"
    stratadb::page_id_t p0 = dm.allocate_page();
    stratadb::page_id_t p1 = dm.allocate_page();
    stratadb::page_id_t p2 = dm.allocate_page();
    check(p0 == 0, "first page has id 0");
    check(p1 == 1, "second page has id 1");
    check(p2 == 2, "third page has id 2");
    check(dm.num_pages() == 3, "num_pages is 3 after 3 allocations");

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
int main() {

    test_disk_manager();

    std::cout << "\nResults: " << test_passed << " passed, " << test_failed << " failed\n";
    
    return test_failed > 0 ? 1 : 0;

}