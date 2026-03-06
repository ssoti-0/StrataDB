#include "index/btree.h"

#include <cstring>
#include <stdexcept>

namespace stratadb {

// Constructor 

BPlusTree::BPlusTree(DiskManager& disk_manager) : disk_manager_(disk_manager), root_page_id_(EMPTY_TREE_SENTINEL) {
    if (disk_manager_.num_pages() == 0) {
        disk_manager_.allocate_page();
    } else {
        root_page_id_ = read_root_page_id();
    }
}

// Metadata page helpers

page_id_t BPlusTree::read_root_page_id() const {
    Page page{};
    disk_manager_.read_page(METADATA_PAGE_ID, page);

    page_id_t root_id = 0;
    std::memcpy(&root_id, page.data(), sizeof(uint32_t));
    return root_id;
}

void BPlusTree::write_root_page_id(page_id_t page_id) {
    Page page{};
    disk_manager_.read_page(METADATA_PAGE_ID, page);

    std::memcpy(page.data(), &page_id, sizeof(uint32_t));
    disk_manager_.write_page(METADATA_PAGE_ID, page);

    root_page_id_ = page_id;
}

// Node I/O helpers

std::unique_ptr<Node> BPlusTree::read_node(page_id_t page_id) const {
    Page page{};
    disk_manager_.read_page(page_id, page);
    return Node::deserialize(page);

}

void BPlusTree::write_node(page_id_t page_id, const Node& node) {
    Page page{};
    node.serialize(page);
    disk_manager_.write_page(page_id, page);
}

// Search

bool BPlusTree::search(int32_t ket, int32_t& value_out) const {
    if (is_empty()) {
        return false;
    }

    page_id_t current_page = root_page_id_;
    auto node = read_node(current_page);

    while (!node->is_leaf()) {
        auto* internal = static_cast<InternalNode*>(node.get());
        int child_idx = internal->find_child_index(key);
        current_page = internal->child_at(child_idx);
        node = read_node(current_page);
    }

    auto* leaf = static_cast<LeafNode*>(node.get());
    int idx = leaf->find_key(key);
    if (idx >= 0) {
        value_out = leaf->value_at(idx);
        return true;
    }
    return false;
}

// Insert - soon

}