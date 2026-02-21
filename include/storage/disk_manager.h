#ifndef STRATADB_DISK_MANAGER_H
#define STRATADB_DISK_MANAGER_H

#include <array>
#include <cstdint>
#include <fstream>
#include <string>

namespace stratadb {

//Every page is 4096 bytes
constexpr std::size_t PAGE_SIZE = 4096;
using Page = std::array<char, PAGE_SIZE>;
using page_id_t = std::uint32_t;

// Page-based I/O
// Main responsibilities:
// - Open or create the database file
// - Read/write fixed-size pages by page_id
// - Allocation of new pages
// - Tracking total page count
//
// Essentially, this is the lowest layer of StrataDB.
class DiskManager {

    private:
    std::string file_path_;
    mutable std::fstream file_;
    page_id_t num_pages_;

    public:
    explicit DiskManager(const std::string& file_path);
    ~DiskManager() = default;

    DiskManager(const DiskManager&) = delete;
    DiskManager& operator=(const DiskManager&) = delete;

    //Reads page and throws a runtime error if page_id >= num_pages, (meaning the page does not exist).
    void read_page(page_id_t page_id, Page& buffer) const;

    //Writes page and allows for page_id == num_pages to append exactly one new page.
    void write_page(page_id_t page_id, const Page& buffer);

    //Allocating a new zero-filled page at the end - returns its page_id
    page_id_t allocate_page();

    page_id_t num_pages() const;
    const std::string& file_path() const;

};
}

#endif 