#ifndef STRATADB_BTREE_H
#define STRATADB_BTREE_H

#include "index/node.h"
#include "storage/disk_manager.h"

#include <cstdint>

namespace stratadb {

    constexpr page_id_t METADATA_PAGE_ID = 0;
    constexpr page_id_t EMPTY_TREE_SENTINEL = 0;

    //Disk-based B+ tree indexx.
    //All of the data lives on disk; nodes are read into memory, modified and written back; 
    //Each operation performs O(log n) reads/writes.

class BPlusTree {
    private:
    DiskManager& disk_manager_;
    page_id_t root_page_id_;

    page_id_t read_root_page_id() const;
    void write_root_page_id(page_id_t page_id);

    std::unique_ptr<Node> read_node(page_id_t page_id) const;
    void write_node(page_id_t page_id, const Node& node);

    public:
    //Init. of B+ tree
    explicit BPlusTree(DiskManager& disk_manager);
    //Searches for key in tree 
    bool search(int32_t key, int32_t& value_out) const;
    //Inserts a key-value pair and if it exists, it replaces the value 
    void insert(int32_t key, int32_t value);

    bool is_empty() const { return root_page_id_ == EMPTY_TREE_SENTINEL; }
    page_id_t root_oage_id() const { return root_page_id_; }
};
}

#endif