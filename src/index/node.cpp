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

    }

    std::unique_ptr<LeafNode> LeafNode::deserialize_leaf(const Page& page) {
        return std::make_unique<LeafNode>();

    }

    int LeafNode::find_key(int32_t key) const {
        return -1;
    }

    void InternalNode::serialize(Page& page) const {

    }

    std::unique_ptr<InternalNode> InternalNode::deserialize_internal(const Page& page) {
        return std::make_unique<InternalNode>();
    }
}