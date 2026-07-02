#ifndef CLUSTER_CONSISTENT_HASH_H
#define CLUSTER_CONSISTENT_HASH_H

#include "node.h"

#include <map>
#include <optional>
#include <string>
#include <vector>

class ConsistentHash
{
public:
    void build(const std::vector<ClusterNode> &nodes, int virtual_nodes = 64);
    std::optional<ClusterNode> owner_for(const std::string &key) const;
    bool empty() const;

private:
    std::map<size_t, ClusterNode> m_ring;
};

#endif
