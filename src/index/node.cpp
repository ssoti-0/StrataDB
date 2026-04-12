#include "index/node.h"
#include "storage/disk_manager.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>


namespace stratadb {
    // check the flag to figure out what kind of node this is  
    std::unique_ptr<Node> Node::deserialize(const Page& page) {
        uint32_t is_leaf_raw = 0;
        std::memcpy(&is_leaf_raw, page.data(), sizeof(uint32_t));
        if (is_leaf_raw == 1) {
            return LeafNode::deserialize_leaf(page);
        } else {
            return InternalNode::deserialize_internal(page);
        }
    }
    // pack leaf data into a page  
    void LeafNode::serialize(Page& page) const {
        page.fill(0);

        std::size_t offset = 0;
        uint32_t is_leaf_raw = 1;
        std::memcpy(page.data() + offset, &is_leaf_raw, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        std::memcpy(page.data() + offset, &num_keys_, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        std::memcpy(page.data() + offset, keys_.data(), ORDER * sizeof(int32_t));
        offset += ORDER * sizeof(int32_t);

        for (int i = 0; i < ORDER; ++i) {
          std::memcpy(page.data() + offset, values_[i].c_str(),std::min(values_[i].size() + 1, MAX_VALUE_SIZE));
          offset += MAX_VALUE_SIZE;
        }

        std::memcpy(page.data() + offset, &next_leaf_, sizeof(uint32_t));
    }
    std::unique_ptr<LeafNode> LeafNode::deserialize_leaf(const Page& page) {
       auto node = std::make_unique<LeafNode>();
       std::size_t offset = 0;
       offset += sizeof(uint32_t);

       for (int i = 0; i < ORDER; ++i) {
          node->values_[i] = std::string(page.data() + offset);
          offset += MAX_VALUE_SIZE;
        }
        if (node->num_keys_ > ORDER) {
            throw std::runtime_error("LeafNode::deserialize: num_keys " + std::to_string(node->num_keys_) + " esceeds ORDER" + std::to_string(ORDER));
        }
    
       std::memcpy(node->keys_.data(), page.data() + offset, ORDER * sizeof(int32_t));
       offset += ORDER * sizeof(int32_t);
       std::memcpy(node->values_.data(), page.data() + offset, ORDER * sizeof(int32_t));
       offset += ORDER * sizeof(int32_t);
       std::memcpy(&node->next_leaf_, page.data() + offset, sizeof(uint32_t));
       return node;
    }

    int LeafNode::find_key(int32_t key) const {
        for (uint32_t i = 0; i < num_keys_; ++i) {
            if (keys_[i] == key) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

    void LeafNode::insert(int32_t key, const std::string& value) {
        int pos = 0;
        while (pos < static_cast<int>(num_keys_) && keys_[pos] < key) {
            ++pos;
    }
    // shift everything right to make room
    for (int i = static_cast<int>(num_keys_); i > pos; --i) {
        keys_[i] = keys_[i-1];
        values_[i] = values_[i-1];
    }

    keys_[pos] = key;
    values_[pos] = value;
    ++num_keys_;
}
bool LeafNode::remove_at(int index) {
    if (index < 0 || index >= static_cast<int>(num_keys_)) {
        return false;
    }
    for (int i = index; i < static_cast<int>(num_keys_) - 1; ++i) {
        keys_[i] = keys_[i + 1];
        values_[i] = values_[i + 1];
    }
    keys_[num_keys_ - 1] = 0;
    values_[num_keys_ - 1].clear();
    --num_keys_;
    return true;
}
void LeafNode::prepend(int32_t key, const std::string& value) {
    for (int i = static_cast<int>(num_keys_); i > 0; --i) {
        keys_[i] = keys_[i - 1];
        values_[i] = values_[i - 1];
    }
    keys_[0] = key;
    values_[0] = value;
    ++num_keys_;
}

std::pair<int32_t, std::string> LeafNode::pop_back() {
    int last = static_cast<int>(num_keys_) - 1;
    auto result = std::make_pair(keys_[last], values_[last]);
    keys_[last] = 0;
    values_[last].clear();
    --num_keys_;
    return result;
}
std::pair<int32_t, std::string> LeafNode::pop_front() {
    auto result = std::make_pair(keys_[0], values_[0]);
    for (int i = 0; i < static_cast<int>(num_keys_) - 1; ++i) {
        keys_[i] = keys_[i + 1];
        values_[i] = values_[i + 1];
    }
    int last = static_cast<int>(num_keys_) - 1;
    keys_[last] = 0;
    values_[last].clear();
    --num_keys_;
    return result;
}

void LeafNode::append_from(const LeafNode& other) {
    for (uint32_t i = 0; i < other.num_keys_; ++i) {
        keys_[num_keys_ + i] = other.keys_[i];
        values_[num_keys_ + i] = other.values_[i];
    }
    num_keys_ += other.num_keys_;
}

// write internal node to page  
void InternalNode::serialize(Page& page) const {
     page.fill(0);

    std::size_t offset = 0;
    uint32_t is_leaf_raw = 0;
    std::memcpy(page.data() + offset, &is_leaf_raw, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    std::memcpy(page.data() + offset, &num_keys_, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    std::memcpy(page.data() + offset, keys_.data(), ORDER * sizeof(int32_t));
    offset += ORDER * sizeof(int32_t);
    std::memcpy(page.data() + offset, children_.data(), (ORDER + 1) * sizeof(uint32_t));
        
}

std::unique_ptr<InternalNode> InternalNode::deserialize_internal(const Page& page) {
    auto node = std::make_unique<InternalNode>();
    std::size_t offset = 0;

    offset += sizeof(uint32_t);
    std::memcpy(&node->num_keys_, page.data() + offset, sizeof(uint32_t));
    offset += sizeof (uint32_t);

    if (node->num_keys_ > ORDER) {
        throw std::runtime_error ("Internal Node:: desserialize: num_keys " + std::to_string(node->num_keys_) + " exceeds ORDER " + std::to_string(ORDER));
    }
    std::memcpy(node->keys_.data(), page.data() + offset, ORDER * sizeof(int32_t));
    offset += ORDER * sizeof(int32_t);
    std::memcpy(node->children_.data(), page.data() + offset, (ORDER + 1) * sizeof(uint32_t));
    return node;
}

int InternalNode::find_child_index(int32_t key) const {
    for (uint32_t i = 0; i < num_keys_; ++i) {
        if (key < keys_[i]) {
            return static_cast<int>(i);
        }
    }
    return static_cast<int>(num_keys_);
}

    // shift keys and children to make space
    void InternalNode::insert_key_child(int32_t key, page_id_t right_child) {
        int pos = 0;
        while (pos < static_cast<int>(num_keys_) && keys_[pos] < key) {
            ++pos;
        }
        for (int i = static_cast<int>(num_keys_); i > pos; --i) {
            keys_[i] = keys_[i-1];
        }
        for (int i = static_cast<int>(num_keys_) + 1; i > pos + 1; --i) {
            children_[i] = children_[i-1];
        }
        keys_[pos] = key;
        children_[pos + 1] = right_child;
        ++num_keys_;
    }

    void InternalNode::remove_key_and_child(int key_index, int child_index) {
      for (int i = key_index; i < static_cast<int>(num_keys_) - 1; ++i) {
          keys_[i] = keys_[i + 1];
      }
      keys_[num_keys_ - 1] = 0;

      for (int i = child_index; i < static_cast<int>(num_keys_); ++i) {
          children_[i] = children_[i + 1];
      }
      children_[num_keys_] = 0;

      --num_keys_;
  }

  void InternalNode::prepend_key_child(int32_t key, page_id_t left_child) {
      for (int i = static_cast<int>(num_keys_); i > 0; --i) {
          keys_[i] = keys_[i - 1];
      }
      for (int i = static_cast<int>(num_keys_) + 1; i > 0; --i) {
          children_[i] = children_[i - 1];
      }
      keys_[0] = key;
      children_[0] = left_child;
      ++num_keys_;
  }

  std::pair<int32_t, page_id_t> InternalNode::pop_back_key_child() {
      int last_key = static_cast<int>(num_keys_) - 1;
      auto result = std::make_pair(keys_[last_key], children_[num_keys_]);
      keys_[last_key] = 0;
      children_[num_keys_] = 0;
      --num_keys_;
      return result;
  }

  std::pair<int32_t, page_id_t> InternalNode::pop_front_key_child() {
      auto result = std::make_pair(keys_[0], children_[0]);
      for (int i = 0; i < static_cast<int>(num_keys_) - 1; ++i) {
          keys_[i] = keys_[i + 1];
      }
      keys_[num_keys_ - 1] = 0;
      for (int i = 0; i < static_cast<int>(num_keys_); ++i) {
          children_[i] = children_[i + 1];
      }
      children_[num_keys_] = 0;
      --num_keys_;
      return result;
  }

  void InternalNode::append_separator_and_node(int32_t separator,
                                               const InternalNode& other) {
      keys_[num_keys_] = separator;
      ++num_keys_;

      for (uint32_t i = 0; i < other.num_keys_; ++i) {
          keys_[num_keys_ + i] = other.keys_[i];
      }
      children_[num_keys_] = other.children_[0];
      for (uint32_t i = 0; i < other.num_keys_; ++i) {
          children_[num_keys_ + 1 + i] = other.children_[i + 1];
      }
      num_keys_ += other.num_keys_;
  }

}