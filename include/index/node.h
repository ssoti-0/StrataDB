#ifndef STRATADB_NODE_H
#define STRATADB_NODE_H

#include "storage/disk_manager.h"

#include <array>
#include <cstdint>
#include <memory>

namespace stratadb {

//Max keys per node. Using 3 for now so splits happen fast during testing.
constexpr int ORDER = 3;

//Each node is one 4096-byte page on disk.
class Node {

protected:
    explicit Node(bool is_leaf) : is_leaf_(is_leaf) {}

    bool is_leaf_; // True if leaf, and false if internal
    uint32_t num_keys_ = 0; // Number of keys currently stored
    std::array<int32_t, ORDER> keys_{}; // Array of keys 

    static constexpr std::size_t HEADER_SIZE = 8;

public:
    virtual ~Node() = default;
    bool is_leaf() const { return is_leaf_;}
    uint32_t num_keys() const { return num_keys_;}
    int32_t key_at (int index) const { return keys_[index]; }

    // Converting node to disk page format 
    virtual void serialize(Page& page) const = 0;

    //Load node from disk page (Creates LeafNode or InternalNode)
    static std::unique_ptr<Node> deserialize(const Page& page); 

};


}

#endif