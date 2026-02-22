#ifndef STRATADB_NODE_H
#define STRATADB_NODE_H

#include "storage/disk_manager.h"

#include <array>
#include <cstdint>
#include <memory>

namespace stratadb {

//Max keys per node. 
constexpr int ORDER = 4;

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
// Leaf Node - stores key-value pairs in sorted oredr  (leaves are linked with next_leaf_)
class LeafNode : public Node {

private:
    std::array<int32_t, ORDER> values_{};
    page_id_t next_leaf_=0; //Page Id of next leaf 

public:
    LeafNode() : Node (true) {}

    int32_t value_at(int index) const { return values_[index]; }
    page_id_t next_leaf() const { return next_leaf_;}

    // Search for a key, return index if found, -1 otherwise
    int find_key(int32_t key) const;

    void insert(int32_t key, int32_t value);

    void serialize(Page& page) const override;
    static std::unique_ptr<LeafNode> deserialize_leaf(const Page& page);
};

// Internal Node - stores keys as separators and child page pointers (for N keys there are N+1 children)
class InternalNode : public Node {

private:
    // ORDER + 1 because we need one more child than keys
    std::array<page_id_t, ORDER +1> children_{};

public: 
    InternalNode() : Node(false) {}

    page_id_t child_at(int index) const {return children_[index];}

    //Find which child to follow for a given key.
    int find_child_index(int32_t key) const;

    void insert_key_child(int32_t key, page_id_t right_child);

    void serialize(Page& page) const override;
    static std::unique_ptr<InternalNode> deserialize_internal(const Page& page);
};

}

#endif