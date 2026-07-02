#ifndef CLUSTER_ROUTER_H
#define CLUSTER_ROUTER_H

#include "consistent_hash.h"

#include "config/server_config.h"
#include "protocol/request.h"

#include <optional>
#include <string>
#include <vector>

class ClusterRouter
{
public:
    bool init(const ServerConfig &config);
    bool enabled() const;
    bool should_route(const Request &request) const;
    bool is_local_owner(const std::string &key) const;
    std::optional<ClusterNode> owner_for(const std::string &key) const;
    std::string forward(const ClusterNode &node, const std::string &line) const;

private:
    static bool is_local_endpoint(const ClusterNode &node, const ServerConfig &config);

private:
    bool m_enabled = false;
    std::string m_local_id;
    ConsistentHash m_hash;
};

#endif
