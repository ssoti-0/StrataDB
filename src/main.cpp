#include "storage/disk_manager.h"

#include <cstring>
#include <iostream>

int main() {

    const std::string test_file = "test.db";
    std::remove(test_file.c_str());

    stratadb::DiskManager dm(test_file);
    std::cout <<"Created DiskManager, pages: " << dm.num_pages() << "\n";

    stratadb::Page buf{};
    const char* msg = "Hello StrataDB";
    std::memcpy(buf.data(), msg, std::strlen(msg));
    dm.write_page(0, buf);

    stratadb::Page read_buf{};
    dm.read_page(0, read_buf);
    std::cout << "Read back: " << read_buf.data() << "\n";

    std::remove(test_file.c_str());
    return 0;
}