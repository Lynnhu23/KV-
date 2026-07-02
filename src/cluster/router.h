#ifndef CLUSTER_ROUTER_H
#define CLUSTER_ROUTER_H

#include "consistent_hash.h"

#include "config/server_config.h"
#include "protocol/request.h"

#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

class ClusterRouter
{
public:
    bool init(const ServerConfig &config);
    bool enabled() const;
    bool raft_enabled() const;
    bool is_leader() const;
    bool should_route(const Request &request) const;
    bool is_local_owner(const std::string &key) const;
    std::optional<ClusterNode> owner_for(const std::string &key) const;
    std::vector<ClusterNode> replicas_for(const std::string &key) const;
    std::vector<ClusterNode> alive_nodes() const;
    std::vector<ClusterNode> raft_followers() const;
    std::optional<ClusterNode> leader_node() const;
    int majority_size() const;
    std::string forward(const ClusterNode &node, const std::string &line) const;

private:
    void refresh_health_if_needed() const;
    bool ping_node(const ClusterNode &node) const;
    void rebuild_hash_locked(const std::vector<ClusterNode> &alive_nodes) const;
    static bool is_local_endpoint(const ClusterNode &node, const ServerConfig &config);

private:
    bool m_enabled = false;
    std::string m_local_id;
    int m_replication_factor = 2;
    int m_health_check_interval_ms = 1000;
    bool m_raft_enabled = false;
    std::string m_leader_id = "node-1";
    std::vector<ClusterNode> m_all_nodes;
    ClusterNode m_local_node;
    mutable std::mutex m_mutex;
    mutable ConsistentHash m_hash;
    mutable std::vector<ClusterNode> m_alive_nodes;
    mutable std::chrono::steady_clock::time_point m_last_health_check{};
};

#endif
