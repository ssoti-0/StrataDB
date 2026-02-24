#include "index/node.h"
#include "storage/disk_manager.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>


namespace stratadb {

    std::unique_ptr<Node> Node::deserialize(const Page& page) {
        uint32_t is_leaf_raw = 0;
        std::memcpy(&is_leaf_raw, page.data(), sizeof(uint32_t));

        if (is_leaf_raw == 1) {
            return LeafNode::deserialize_leaf(page);
        } else {
            return InternalNode::deserialize_internal(page);
        }
    }

    void LeafNode::serialize(Page& page) const {
        
        std::size_t offset = 0;
        uint32_t is_leaf_raw = 1;
        std::memcpy(page.data() + offset, &is_leaf_raw, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        std::memcpy(page.data() + offset, &num_keys_, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        std::memcpy(page.data() + offset, keys_.data(), ORDER * sizeof(int32_t));
        offset += ORDER * sizeof(int32_t);

        std::memcpy(page.data() + offset, values_.data(), ORDER * sizeof(int32_t));
        offset += ORDER * sizeof(int32_t);

        std::memcpy(page.data() + offset, &next_leaf_, sizeof(uint32_t));
    }

    std::unique_ptr<LeafNode> LeafNode::deserialize_leaf(const Page& page) {
       auto node = std::make_unique<LeafNode>();
       std::size_t offset = 0;

       offset += sizeof(uint32_t);

       std::memcpy(&node->num_keys_, page.data() + offset, sizeof(uint32_t));
       offset += sizeof(uint32_t);

        if (node->num_keys_ > ORDER) {
            throw std::runtime_error("LeafNode::deserialize: num_keys " + std::to_string(node->num_keys_) + " esceeds ORDER" + std::to_string(ORDER));
        }
    
       std::memcpy(node->keys_.data(), page.data() + offset, ORDER * sizeof(int32_t));
 
       std::memcpy(node->values_.data(), page.data() + offset, ORDER * sizeof(int32_t));
       offset += ORDER * sizeof(int32_t);

       std::memcpy(&node->next_leaf_, page.data() + offset, sizeof(uint32_t));

       return node;
    }

    int LeafNode::find_key(int32_t key) const {
        //To do
        return -1;
    }

    void InternalNode::serialize(Page& page) const {
         //To do

    }

    std::unique_ptr<InternalNode> InternalNode::deserialize_internal(const Page& page) {
        return std::make_unique<InternalNode>();
         //To do
    }
}