#include "consistent_hash.h"

#include <functional>

void ConsistentHash::build(const std::vector<ClusterNode> &nodes, int virtual_nodes)
{
    m_ring.clear();
    std::hash<std::string> hasher;

    for (const auto &node : nodes)
    {
        for (int i = 0; i < virtual_nodes; ++i)
        {
            std::string vnode_key = node.id + "#" + std::to_string(i);
            m_ring.emplace(hasher(vnode_key), node);
        }
    }
}

std::optional<ClusterNode> ConsistentHash::owner_for(const std::string &key) const
{
    if (m_ring.empty())
    {
        return std::nullopt;
    }

    std::hash<std::string> hasher;
    auto it = m_ring.lower_bound(hasher(key));
    if (it == m_ring.end())
    {
        it = m_ring.begin();
    }
    return it->second;
}

bool ConsistentHash::empty() const
{
    return m_ring.empty();
}
