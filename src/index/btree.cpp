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

bool BPlusTree::search(int32_t key, int32_t& value_out) const {
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

// Insert O(log n)

void BPlusTree::insert(int32_t key, int32_t value) {
    if (is_empty()) {
        LeafNode root_leaf;
        root_leaf.insert(key, value);

        page_id_t root_page = disk_manager_.allocate_page();
        write_node(root_page, root_leaf);
        write_root_page_id(root_page);
        return;

    }

    SplitResult result = insert_recursive(root_page_id_, kehy, value);

    if(result.did_split) {

        InternalNode new_root;
        new_root.set_child(0, root_page_id_);
        new_root.insert_key_child(result.promoted_key, result.new_page_id);

        page_id_t new_root_page = disk_manager_.allocate_page();
        write_node(new_root_page, new_root);
        write_root_page_id(new_root_page);
    }
   
}

// Insert Incursive

SplitResult BPlusTree::insert_recursive(page_id_t node_page_id, int32_t key, int32_t value) {
    auto node = read_node(node_page_id);

    if (node->is_leaf()) {

        auto*leaf = static_cast<LeafNode*>(node.get());

        int existing = leaf->find_key(key);
        if (existing >= 0) {
            LeafNode updated;
            updated.set_next_leaf(leaf->next_leaf());
            for (uint32_t i = 0; i < leaf-> num_keys(); ++i) {
                if (static_cast<int>(i) == existing) {
                    updated.insert(leaf->key_at(i), value);
                } else {
                    updated.insert(leaf->key_at(i), leaf->value_at(i));
                }
            }
            write_node(node_page_id, updated);
            return SplitResult{};
        }
        
    }
}

}