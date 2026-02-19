#include "storage/disk_manager.h"
#include <stdexcept>

namespace stratadb {

    DiskManager::DiskManager (const std::string& file_path)
    : file_path_(file_path), num_pages_(0) {

       
        file_.open(file_path_, std::ios::in | std::ios::binary | std::ios::ate);

        if (!file_.is_open()) {

            std::ofstream create_file(file_path_, std::ios::binary);
            if (!create_file.is_open()) {
                throw std::runtime_error(
                    "DiskManager: failed to create file '" + file_path_ + "'");
            }
            create_file.close();

            file_.open(file_path_, std::ios::in | std::ios::out | std::ios::binary);

            if (!file_.is_open()) {
                throw std::runtime_error ("DiskManager: failed to open file '" + file_path_ + "' after craetion");
            }
            num_pages_ = 0;
        } else {

            auto file_size = file_.tellg();
            file_.close();

            num_pages_ = static_cast<page_id_t>(file_size) / PAGE_SIZE;

            file_.open(file_path_, std::ios::in | std::ios::out |std::ios::binary);
            if (!file_.is_open()) {
                throw std::runtime_error (
                    "DiskManager: failed to reopen file '" + file_path_ + "'");
                
            }
        }
    }
}