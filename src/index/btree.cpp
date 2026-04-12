#include "index/btree.h"

#include <cstring>
#include <stdexcept>



// BPlus Tree implementation 
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

// Search O(log n)

bool BPlusTree::search(int32_t key, std::string& value_out) const {
      if (is_empty()) {
          return false;
      }

      page_id_t current_page = root_page_id_;
      auto node = read_node(current_page);

      // walk down from root to the right leaf      
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
std::vector<std::pair<int32_t, std::string>> BPlusTree::scan_all() const {   
      std::vector<std::pair<int32_t, std::string>> results;

    if (is_empty()) {
        return results;
    }

    auto node = read_node(root_page_id_);
    // go to the leftmost leaf 
    while (!node->is_leaf()) {
        auto* internal = static_cast<InternalNode*>(node.get());
        node = read_node(internal->child_at(0));
    }
    // follow the leaf chain
    while (true) {
        auto* leaf = static_cast<LeafNode*>(node.get());
        for (uint32_t i = 0; i < leaf->num_keys(); i++) {
            results.push_back({leaf->key_at(i), leaf->value_at(i)});
        }

        page_id_t next = leaf->next_leaf();
        if (next == 0) {
            break;
        }
        node = read_node(next);
    }
    return results;
}


bool BPlusTree::delete_key(int32_t key) {
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
      if (idx < 0) {
          return false;  
      }

      leaf->remove_at(idx);
      write_node(current_page, *leaf);
      return true;
}



// Insert O(log n)

void BPlusTree::insert(int32_t key, const std::string& value) {
    // first insert — create a leaf as root
    if (is_empty()) {
        LeafNode root_leaf;
        root_leaf.insert(key, value);

        page_id_t root_page = disk_manager_.allocate_page();
        write_node(root_page, root_leaf);
        write_root_page_id(root_page);
        return;

    }

    SplitResult result = insert_recursive(root_page_id_, key, value);
    // root split — need a new root above the two halves
    if(result.did_split) {

        InternalNode new_root;
        new_root.set_child(0, root_page_id_);
        new_root.insert_key_child(result.promoted_key, result.new_page_id);

        page_id_t new_root_page = disk_manager_.allocate_page();
        write_node(new_root_page, new_root);
        write_root_page_id(new_root_page);
    }
   
}

// Insert Recursive

SplitResult BPlusTree::insert_recursive(page_id_t node_page_id,int32_t key, const std::string& value) {
    auto node = read_node(node_page_id);

    if (node->is_leaf()) {

        auto*leaf = static_cast<LeafNode*>(node.get());
        // if key already exists, update the value
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

        if (!leaf->is_full()) {
            leaf->insert(key, value);
            write_node(node_page_id, *leaf);
            return SplitResult{};
        }
        // leaf is full, need to split
        return split_leaf(*leaf, node_page_id, key, value);
    }

    auto* internal = static_cast<InternalNode*>(node.get());

    int child_idx = internal->find_child_index(key);
    page_id_t child_page = internal->child_at(child_idx);

    SplitResult child_result = insert_recursive(child_page, key, value);

    if (!child_result.did_split) {
        
        return SplitResult{}; 
    }

    if (!internal->is_full()) {
        internal->insert_key_child(child_result.promoted_key, child_result.new_page_id);

        write_node(node_page_id, *internal);
        return SplitResult{};
    }

    return split_internal(*internal, node_page_id, child_result.promoted_key, child_result.new_page_id);




}

SplitResult BPlusTree::split_leaf(LeafNode& leaf, page_id_t leaf_page_id,int32_t key, const std::string& value) {
    std::array<int32_t, ORDER + 1> all_keys{};
    std::array<std::string, ORDER + 1> all_values{};

    int insert_pos = 0;
    while (insert_pos < ORDER && leaf.key_at(insert_pos) < key) {
        ++insert_pos;
    }
    for (int i = 0, src = 0; i < ORDER + 1; ++i) {
        if (i == insert_pos) {
            all_keys[i] = key;
            all_values[i] = value;
        } else {
            all_keys[i] = leaf.key_at(src);
            all_values[i] = leaf.value_at(src);
            ++src;
        }
    }
    // split at the middle
    int mid = (ORDER + 1) / 2;

    LeafNode left;
    LeafNode right;

    for (int i = 0; i < mid; ++i) {
        left.insert(all_keys[i], all_values[i]);
    }
    for (int i = mid; i < ORDER + 1; ++i) {
        right.insert(all_keys[i], all_values[i]);
    }

    page_id_t right_page = disk_manager_.allocate_page();
    // fix the leaf chain pointers
    right.set_next_leaf(leaf.next_leaf());
    left.set_next_leaf(right_page);
    write_node(leaf_page_id, left);
    write_node(right_page, right);

    return SplitResult{true, all_keys[mid], right_page};
}

SplitResult BPlusTree::split_internal(InternalNode& node,page_id_t node_page_id,int32_t key,page_id_t right_child) {
    std::array<int32_t, ORDER + 1> all_keys{};
    std::array<page_id_t, ORDER + 2> all_children{};

    int insert_pos = 0;
    while (insert_pos < ORDER && node.key_at(insert_pos) < key) {
        ++insert_pos;
    }
    // merge existing keys + new key into one sorted array
    for (int i = 0, src = 0; i < ORDER + 1; ++i) {
        if (i == insert_pos) {
            all_keys[i] = key;
        } else {
            all_keys[i] = node.key_at(src);
            ++src;
        }
    }

    for (int i = 0, src = 0; i < ORDER + 2; ++i) {
        if (i == insert_pos + 1) {
            all_children[i] = right_child;
        } else {
            all_children[i] = node.child_at(src);
            ++src;
        }
    }

    int mid = (ORDER + 1) / 2;
    int32_t promoted_key = all_keys[mid];
    // divide into two halves, middle key goes up to parent
    InternalNode left;
    InternalNode right_node;

    left.set_child(0, all_children[0]);
    for (int i = 0; i < mid; ++i) {
        left.insert_key_child(all_keys[i], all_children[i + 1]);
    }

    right_node.set_child(0, all_children[mid + 1]);
    for (int i = mid + 1; i < ORDER + 1; ++i) {
        right_node.insert_key_child(all_keys[i], all_children[i + 1]);
    }

    page_id_t right_page = disk_manager_.allocate_page();
    write_node(node_page_id, left);
    write_node(right_page, right_node);

    return SplitResult{true, promoted_key, right_page};
}
}