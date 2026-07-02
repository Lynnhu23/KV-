#ifndef CLUSTER_ROUTER_H
#define CLUSTER_ROUTER_H

#include "consistent_hash.h"

#include "config/server_config.h"
#include "protocol/request.h"

#include <chrono>
#include <atomic>
#include <mutex>
#include <optional>
#include <thread>
#include <string>
#include <vector>

class ClusterRouter
{
public:
    bool init(const ServerConfig &config);
    bool enabled() const;
    std::string local_id() const;
    bool raft_enabled() const;
    bool is_leader() const;
    std::string role_name() const;
    std::string leader_id() const;
    int current_term() const;
    long long election_count() const;
    void update_log_info(int last_log_index, int last_log_term);
    void stop();
    bool should_route(const Request &request) const;
    bool is_local_owner(const std::string &key) const;
    std::optional<ClusterNode> owner_for(const std::string &key) const;
    std::vector<ClusterNode> replicas_for(const std::string &key) const;
    std::vector<ClusterNode> alive_nodes() const;
    std::vector<ClusterNode> raft_followers() const;
    std::optional<ClusterNode> leader_node() const;
    int majority_size() const;
    std::string forward(const ClusterNode &node, const std::string &line) const;
    std::string handle_request_vote(int term,
                                    const std::string &candidate_id,
                                    int candidate_last_log_index,
                                    int candidate_last_log_term);
    std::string handle_append_entries(int term, const std::string &leader_id);

private:
    enum class RaftRole
    {
        Follower,
        Candidate,
        Leader
    };

    void refresh_health_if_needed() const;
    bool ping_node(const ClusterNode &node) const;
    void rebuild_hash_locked(const std::vector<ClusterNode> &alive_nodes) const;
    void raft_loop(std::stop_token token);
    void start_election();
    void send_heartbeats();
    bool parse_vote_response(const std::string &response, int &term, bool &granted) const;
    bool parse_append_response(const std::string &response, int &term, bool &success) const;
    int election_timeout_ms() const;
    bool load_raft_meta_locked(const std::string &data_dir);
    bool persist_raft_meta_locked() const;
    static bool is_local_endpoint(const ClusterNode &node, const ServerConfig &config);

private:
    bool m_enabled = false;
    std::string m_local_id;
    int m_replication_factor = 2;
    int m_health_check_interval_ms = 1000;
    bool m_raft_enabled = false;
    std::string m_leader_id = "node-1";
    std::atomic<bool> m_stopping{false};
    std::vector<ClusterNode> m_all_nodes;
    ClusterNode m_local_node;
    mutable std::mutex m_mutex;
    mutable ConsistentHash m_hash;
    mutable std::vector<ClusterNode> m_alive_nodes;
    mutable std::chrono::steady_clock::time_point m_last_health_check{};
    std::jthread m_raft_thread;
    mutable RaftRole m_role = RaftRole::Follower;
    mutable int m_current_term = 0;
    mutable long long m_election_count = 0;
    mutable std::string m_voted_for;
    mutable int m_last_log_index = 0;
    mutable int m_last_log_term = 0;
    std::string m_raft_meta_file;
    mutable std::chrono::steady_clock::time_point m_last_heartbeat{};
    mutable int m_election_timeout_ms = 1000;
};

#endif
