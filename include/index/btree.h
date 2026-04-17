#ifndef STRATADB_BTREE_H
#define STRATADB_BTREE_H

#include "index/node.h"
#include "storage/disk_manager.h"

#include <cstdint>
#include <utility>
#include <vector>

namespace stratadb {

    constexpr page_id_t METADATA_PAGE_ID = 0;
    constexpr page_id_t EMPTY_TREE_SENTINEL = 0;

    //Disk-based B+ tree indexx.
    //All of the data lives on disk; nodes are read into memory, modified and written back; 
    //Each operation performs O(log n) reads/writes.

struct SplitResult {
    bool did_split = false;
    int32_t promoted_key = 0;
    page_id_t new_page_id = 0;
};
struct DeleteResult {
    bool key_found = false;
    bool did_underflow = false;
};

class BPlusTree {
    private:
    DiskManager& disk_manager_;
    page_id_t root_page_id_;
     uint64_t split_count_ = 0;
    uint64_t merge_count_ = 0;
    uint64_t redistribute_count_ = 0;

    page_id_t read_root_page_id() const;
    void write_root_page_id(page_id_t page_id);

    std::unique_ptr<Node> read_node(page_id_t page_id) const;
    void write_node(page_id_t page_id, const Node& node);
    // Insert algorithms
    // Recursively inserts into the subtree rooted at note_page.
    SplitResult insert_recursive(page_id_t node_page_id,int32_t key, const std::string& value);  
    // SPlits a full leaf and returns a new promoted key and new right page
    SplitResult split_leaf(LeafNode& leaf, page_id_t leaf_page_id,int32_t key, const std::string& value);
    // Splits a full inernal node and returns promoted key and new right page 
    SplitResult split_internal(InternalNode& node, page_id_t node_page_id, int32_t key, page_id_t right_child);

    DeleteResult del_recursive(page_id_t page, int32_t key);

    void fix_underflow(InternalNode& parent, page_id_t parent_page, int idx);
    void borrow_leaf_right(LeafNode& leaf, page_id_t leaf_page,LeafNode& right, page_id_t right_page,InternalNode& parent, int sep);
    void borrow_leaf_left(LeafNode& leaf, page_id_t leaf_page,LeafNode& left, page_id_t left_page,InternalNode& parent, int sep);
    void merge_leaf(LeafNode& left, page_id_t left_page,LeafNode& right, page_id_t right_page,InternalNode& parent, page_id_t parent_page, int sep);
    void borrow_internal_right(InternalNode& node, page_id_t node_page,InternalNode& right, page_id_t right_page,InternalNode& parent, int sep);
    void borrow_internal_left(InternalNode& node, page_id_t node_page,InternalNode& left, page_id_t left_page,InternalNode& parent, int sep);
    void merge_internal(InternalNode& left, page_id_t left_page,InternalNode& right, page_id_t right_page,InternalNode& parent, page_id_t parent_page, int sep);


    public:
    //Init. of B+ tree
    explicit BPlusTree(DiskManager& disk_manager);
    //Searches for key in tree 
    bool search(int32_t key, std::string& value_out) const;  
    //Inserts a key-value pair and if it exists, it replaces the value 
    void insert(int32_t key, const std::string& value);

    bool delete_key(int32_t key);

    std::vector<std::pair<int32_t, std::string>> scan_all() const; 
    bool is_empty() const { return root_page_id_ == EMPTY_TREE_SENTINEL; }
    page_id_t root_page_id() const { return root_page_id_; }

    // B+ tree operation statistics
    uint64_t split_count() const { return split_count_; }
    uint64_t merge_count() const { return merge_count_; }
    uint64_t redistribute_count() const { return redistribute_count_; }
    void reset_stats() { split_count_ = 0; merge_count_ = 0; redistribute_count_ = 0; }
};
}

#endif