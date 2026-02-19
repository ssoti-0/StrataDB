#include "storage/disk_manager.h"
#include <stdexcept>

namespace stratadb {

    DiskManager::DiskManager (const std::string& file_path)
    : file_path_(file_path), num_pages_(0) {

       //Trying to open the file first 
        file_.open(file_path_, std::ios::in | std::ios::binary | std::ios::ate);

        if (!file_.is_open()) {
            //If file doesn't exist - create it.
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
            // If file exists - compute the page count.
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
void DiskManager::read_page(page_id_t page_id, Page& buffer) const {
    if (page_id >= num_pages_) {
        throw std::runtime_error ("read_page: page_id " + std::to_string(page_id)+ " out of range");

    }

    auto offset = static_cast<std::streamoff>(page_id) * PAGE_SIZE;
    file_.seekg(offset);
    if (!file_.good()) {
        throw std::runtime_error("read_page: seek failed");
    }

    file_.read(buffer.data(), PAGE_SIZE);
    if(!file_.good()) {
        throw std::runtime_error("read_page: read failed");
    }
}

void DiskManager::write_page(page_id_t page_id, const Page& buffer) {
    if (page_id >= num_pages_) {
        throw std::runtime_error("write_page: page_id " + std::to_string(page_id) + " out of range");
    }

    auto offset = static_cast<std::streamoff>(page_id) * PAGE_SIZE;
    file_.seekp(offset);
    if (!file_.good()) {
        throw std::runtime_error("write_page: seek failed");
    }

    if (page_id == num_pages_) {
        ++num_pages_;
    }
}
page_id_t DiskManager::num_pages() const {
    return num_pages_;
}

const std::string& DiskManager::file_path() const {
    return file_path_;
}
}