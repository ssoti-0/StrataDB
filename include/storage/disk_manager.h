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

//Page-based I/O
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

    //Reads page (throws if page_id out of range)
    void read_page(page_id_t page_id, Page& buffer) const;

    //Writes page (throws if page_id out of range)
    void write_page(page_id_t page_id, const Page& buffer);

    page_id_t num_pages() const;
    const std::string& file_path() const;

};
}

#endif 