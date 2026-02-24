#include "index/node.h"
#include "storage/disk_manager.h"

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
        return std::make_unique<LeafNode>();
         //To do
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