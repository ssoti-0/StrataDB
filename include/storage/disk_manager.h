#ifndef STRATADB_DISK_MANAGER_H
#define STRATADB_DISK_MANAGER_H

#include <array>
#include <cstdint>
#include <fstream>
#include <string>

namespace stratadb {

    //Every page is 4096 bytes
    constexpr std::size_t PAGE_SIZE = 4096;
    using PAGE = std::array<char, PAGE_SIZE>;
    using page_id_t = std::uint32_t;


class DiskManager {

    private:
    std::string file_path_;
    mutable std::fstream file_;
    page_id_t num_pages_;

    public:
    
};
}

#endif 