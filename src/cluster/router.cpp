#include "router.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>
#include <thread>

bool ClusterRouter::init(const ServerConfig &config)
{
    m_enabled = config.cluster.enabled;
    m_local_id = config.node.id;
    m_replication_factor = config.cluster.replication_factor;
    m_health_check_interval_ms = config.cluster.health_check_interval_ms;
    m_raft_enabled = config.cluster.consistency == "raft";
    m_leader_id = config.cluster.leader_id;
    m_local_node = {config.node.id, "127.0.0.1", config.node.port};

    if (!m_enabled)
    {
        return true;
    }

    m_all_nodes.clear();
    for (const auto &peer : config.cluster.peers)
    {
        m_all_nodes.push_back({peer.id, peer.host, peer.port});
    }

    bool has_local = false;
    for (const auto &node : m_all_nodes)
    {
        if (node.id == config.node.id || is_local_endpoint(node, config))
        {
            has_local = true;
            m_local_node = {config.node.id, node.host, node.port};
            break;
        }
    }

    if (!has_local)
    {
        m_all_nodes.push_back(m_local_node);
    }

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_alive_nodes = {m_local_node};
        m_hash.build(m_alive_nodes);
        m_last_health_check = std::chrono::steady_clock::now() -
                              std::chrono::milliseconds(m_health_check_interval_ms);
        m_role = RaftRole::Follower;
        m_raft_meta_file = config.node.data_dir + "/raft.meta";
        load_raft_meta_locked(config.node.data_dir);
        if (m_raft_enabled)
        {
            m_leader_id.clear();
        }
        m_last_heartbeat = std::chrono::steady_clock::now();
        m_election_timeout_ms = election_timeout_ms();
    }
    refresh_health_if_needed();
    if (m_raft_enabled)
    {
        m_stopping = false;
        m_raft_thread = std::jthread([this](std::stop_token token) {
            raft_loop(token);
        });
    }
    return !m_hash.empty();
}

bool ClusterRouter::enabled() const
{
    return m_enabled;
}

std::string ClusterRouter::local_id() const
{
    return m_local_id;
}

bool ClusterRouter::raft_enabled() const
{
    return m_enabled && m_raft_enabled;
}

bool ClusterRouter::is_leader() const
{
    if (!raft_enabled())
    {
        return true;
    }
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_role == RaftRole::Leader;
}

std::string ClusterRouter::role_name() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    switch (m_role)
    {
    case RaftRole::Leader:
        return "leader";
    case RaftRole::Candidate:
        return "candidate";
    case RaftRole::Follower:
        return "follower";
    }
    return "unknown";
}

std::string ClusterRouter::leader_id() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_leader_id;
}

int ClusterRouter::current_term() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_current_term;
}

long long ClusterRouter::election_count() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_election_count;
}

void ClusterRouter::update_log_info(int last_log_index, int last_log_term)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_last_log_index = last_log_index;
    m_last_log_term = last_log_term;
}

void ClusterRouter::stop()
{
    m_stopping = true;
    if (m_raft_thread.joinable())
    {
        m_raft_thread.request_stop();
    }
}

bool ClusterRouter::should_route(const Request &request) const
{
    if (!m_enabled)
    {
        return false;
    }

    return request.type == CommandType::Put ||
           request.type == CommandType::Get ||
           request.type == CommandType::Del ||
           request.type == CommandType::Exists ||
           request.type == CommandType::Expire ||
           request.type == CommandType::Ttl;
}

bool ClusterRouter::is_local_owner(const std::string &key) const
{
    auto owner = owner_for(key);
    return !owner.has_value() || owner->id == m_local_id;
}

std::optional<ClusterNode> ClusterRouter::owner_for(const std::string &key) const
{
    if (!m_enabled)
    {
        return std::nullopt;
    }
    refresh_health_if_needed();
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_hash.owner_for(key);
}

std::vector<ClusterNode> ClusterRouter::replicas_for(const std::string &key) const
{
    if (!m_enabled || m_replication_factor <= 1)
    {
        return {};
    }

    refresh_health_if_needed();
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_alive_nodes.size() <= 1)
    {
        return {};
    }

    auto owner = m_hash.owner_for(key);
    if (!owner.has_value())
    {
        return {};
    }

    std::vector<ClusterNode> sorted = m_alive_nodes;
    std::sort(sorted.begin(), sorted.end(), [](const ClusterNode &a, const ClusterNode &b) {
        return a.id < b.id;
    });

    auto owner_it = std::find_if(sorted.begin(), sorted.end(), [&](const ClusterNode &node) {
        return node.id == owner->id;
    });
    if (owner_it == sorted.end())
    {
        return {};
    }

    std::vector<ClusterNode> replicas;
    size_t owner_index = static_cast<size_t>(owner_it - sorted.begin());
    size_t target = std::min(static_cast<size_t>(m_replication_factor - 1), sorted.size() - 1);
    for (size_t offset = 1; replicas.size() < target && offset < sorted.size(); ++offset)
    {
        const auto &node = sorted[(owner_index + offset) % sorted.size()];
        if (node.id != m_local_id)
        {
            replicas.push_back(node);
        }
    }
    return replicas;
}

std::vector<ClusterNode> ClusterRouter::alive_nodes() const
{
    if (!m_enabled)
    {
        return {};
    }

    refresh_health_if_needed();
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_alive_nodes;
}

std::vector<ClusterNode> ClusterRouter::raft_followers() const
{
    if (!raft_enabled())
    {
        return {};
    }

    refresh_health_if_needed();
    std::lock_guard<std::mutex> lock(m_mutex);
    std::vector<ClusterNode> followers;
    for (const auto &node : m_alive_nodes)
    {
        if (node.id != m_local_id)
        {
            followers.push_back(node);
        }
    }
    return followers;
}

std::optional<ClusterNode> ClusterRouter::leader_node() const
{
    if (!raft_enabled())
    {
        return std::nullopt;
    }

    std::string leader_id;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        leader_id = m_leader_id;
    }
    if (leader_id.empty())
    {
        return std::nullopt;
    }

    for (const auto &node : m_all_nodes)
    {
        if (node.id == leader_id)
        {
            return node;
        }
    }
    return std::nullopt;
}

int ClusterRouter::majority_size() const
{
    if (!raft_enabled())
    {
        return 1;
    }
    return static_cast<int>(m_all_nodes.size() / 2 + 1);
}

std::string ClusterRouter::forward(const ClusterNode &node, const std::string &line) const
{
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo *result = nullptr;
    std::string port = std::to_string(node.port);
    if (getaddrinfo(node.host.c_str(), port.c_str(), &hints, &result) != 0)
    {
        return "ERROR route_unavailable\n";
    }

    int fd = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (fd < 0)
    {
        freeaddrinfo(result);
        return "ERROR route_unavailable\n";
    }

    timeval timeout{};
    timeout.tv_sec = 2;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    if (connect(fd, result->ai_addr, result->ai_addrlen) < 0)
    {
        close(fd);
        freeaddrinfo(result);
        return "ERROR route_unavailable\n";
    }
    freeaddrinfo(result);

    std::string request_line = line;
    if (request_line.empty() || request_line.back() != '\n')
    {
        request_line.push_back('\n');
    }

    ssize_t sent = send(fd, request_line.data(), request_line.size(), 0);
    if (sent < 0 || static_cast<size_t>(sent) != request_line.size())
    {
        close(fd);
        return "ERROR route_unavailable\n";
    }
    shutdown(fd, SHUT_WR);

    std::string response;
    char buffer[4096];
    while (true)
    {
        ssize_t n = recv(fd, buffer, sizeof(buffer), 0);
        if (n > 0)
        {
            response.append(buffer, static_cast<size_t>(n));
            continue;
        }
        break;
    }

    close(fd);
    if (response.empty())
    {
        return "ERROR route_unavailable\n";
    }
    return response;
}

void ClusterRouter::refresh_health_if_needed() const
{
    if (!m_enabled)
    {
        return;
    }

    auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - m_last_health_check).count();
        if (elapsed < m_health_check_interval_ms)
        {
            return;
        }
        m_last_health_check = now;
    }

    std::vector<ClusterNode> alive;
    for (const auto &node : m_all_nodes)
    {
        if (node.id == m_local_id || node.port == m_local_node.port)
        {
            alive.push_back(node);
            continue;
        }
        if (ping_node(node))
        {
            alive.push_back(node);
        }
    }

    if (alive.empty())
    {
        alive.push_back(m_local_node);
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    rebuild_hash_locked(alive);
}

bool ClusterRouter::ping_node(const ClusterNode &node) const
{
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo *result = nullptr;
    std::string port = std::to_string(node.port);
    if (getaddrinfo(node.host.c_str(), port.c_str(), &hints, &result) != 0)
    {
        return false;
    }

    int fd = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (fd < 0)
    {
        freeaddrinfo(result);
        return false;
    }

    timeval timeout{};
    timeout.tv_sec = 0;
    timeout.tv_usec = 200000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    if (connect(fd, result->ai_addr, result->ai_addrlen) < 0)
    {
        close(fd);
        freeaddrinfo(result);
        return false;
    }
    freeaddrinfo(result);

    const std::string request = "PING\n";
    if (send(fd, request.data(), request.size(), 0) != static_cast<ssize_t>(request.size()))
    {
        close(fd);
        return false;
    }
    shutdown(fd, SHUT_WR);

    char buffer[64];
    ssize_t n = recv(fd, buffer, sizeof(buffer), 0);
    close(fd);
    if (n <= 0)
    {
        return false;
    }

    std::string response(buffer, static_cast<size_t>(n));
    return response == "PONG\n" || response == "+PONG\r\n";
}

std::string ClusterRouter::handle_request_vote(int term,
                                               const std::string &candidate_id,
                                               int candidate_last_log_index,
                                               int candidate_last_log_term)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (term < m_current_term)
    {
        return "RAFT_VOTE " + std::to_string(m_current_term) + " 0\n";
    }

    if (term > m_current_term)
    {
        m_current_term = term;
        m_role = RaftRole::Follower;
        m_voted_for.clear();
        m_leader_id.clear();
        persist_raft_meta_locked();
    }

    bool granted = false;
    bool log_is_up_to_date =
        candidate_last_log_term > m_last_log_term ||
        (candidate_last_log_term == m_last_log_term && candidate_last_log_index >= m_last_log_index);
    if (log_is_up_to_date && (m_voted_for.empty() || m_voted_for == candidate_id))
    {
        granted = true;
        m_voted_for = candidate_id;
        m_role = RaftRole::Follower;
        m_last_heartbeat = std::chrono::steady_clock::now();
        m_election_timeout_ms = election_timeout_ms();
        persist_raft_meta_locked();
    }

    return "RAFT_VOTE " + std::to_string(m_current_term) + " " + (granted ? "1" : "0") + "\n";
}

std::string ClusterRouter::handle_append_entries(int term, const std::string &leader_id)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (term < m_current_term)
    {
        return "RAFT_APPEND " + std::to_string(m_current_term) + " 0\n";
    }

    if (term > m_current_term)
    {
        m_current_term = term;
        m_voted_for.clear();
        persist_raft_meta_locked();
    }

    m_role = RaftRole::Follower;
    m_leader_id = leader_id;
    m_last_heartbeat = std::chrono::steady_clock::now();
    m_election_timeout_ms = election_timeout_ms();
    return "RAFT_APPEND " + std::to_string(m_current_term) + " 1\n";
}

void ClusterRouter::raft_loop(std::stop_token token)
{
    auto last_heartbeat_sent = std::chrono::steady_clock::now();
    while (!token.stop_requested() && !m_stopping)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (!raft_enabled())
        {
            continue;
        }

        bool leader = false;
        bool election_due = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            auto now = std::chrono::steady_clock::now();
            leader = m_role == RaftRole::Leader;
            election_due = !leader &&
                           std::chrono::duration_cast<std::chrono::milliseconds>(now - m_last_heartbeat).count() >=
                               m_election_timeout_ms;
        }

        auto now = std::chrono::steady_clock::now();
        if (leader)
        {
            if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat_sent).count() >= 300)
            {
                send_heartbeats();
                last_heartbeat_sent = now;
            }
            continue;
        }

        if (election_due)
        {
            start_election();
            last_heartbeat_sent = std::chrono::steady_clock::now();
        }
    }
}

void ClusterRouter::start_election()
{
    int term = 0;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_role = RaftRole::Candidate;
        ++m_current_term;
        ++m_election_count;
        term = m_current_term;
        m_voted_for = m_local_id;
        m_leader_id.clear();
        m_last_heartbeat = std::chrono::steady_clock::now();
        m_election_timeout_ms = election_timeout_ms();
        persist_raft_meta_locked();
    }

    int votes = 1;
    const std::string request = "RAFT_REQUEST_VOTE " + std::to_string(term) + " " + m_local_id + " " +
                                std::to_string(m_last_log_index) + " " +
                                std::to_string(m_last_log_term) + "\n";
    for (const auto &node : m_all_nodes)
    {
        if (node.id == m_local_id || node.port == m_local_node.port)
        {
            continue;
        }

        int response_term = 0;
        bool granted = false;
        if (parse_vote_response(forward(node, request), response_term, granted))
        {
            if (response_term > term)
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (response_term > m_current_term)
                {
                    m_current_term = response_term;
                    m_role = RaftRole::Follower;
                    m_voted_for.clear();
                    m_leader_id.clear();
                    persist_raft_meta_locked();
                }
                return;
            }
            if (granted)
            {
                ++votes;
            }
        }

        if (votes >= majority_size())
        {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_role == RaftRole::Candidate && m_current_term == term)
                {
                    m_role = RaftRole::Leader;
                    m_leader_id = m_local_id;
                    m_last_heartbeat = std::chrono::steady_clock::now();
                }
            }
            send_heartbeats();
            return;
        }
    }
}

void ClusterRouter::send_heartbeats()
{
    int term = 0;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_role != RaftRole::Leader)
        {
            return;
        }
        term = m_current_term;
        m_leader_id = m_local_id;
    }

    const std::string request = "RAFT_APPEND_ENTRIES " + std::to_string(term) + " " + m_local_id + "\n";
    for (const auto &node : m_all_nodes)
    {
        if (node.id == m_local_id || node.port == m_local_node.port)
        {
            continue;
        }

        int response_term = 0;
        bool success = false;
        if (parse_append_response(forward(node, request), response_term, success) && response_term > term)
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (response_term > m_current_term)
            {
                m_current_term = response_term;
                m_role = RaftRole::Follower;
                m_voted_for.clear();
                m_leader_id.clear();
                persist_raft_meta_locked();
            }
            return;
        }
    }
}

bool ClusterRouter::parse_vote_response(const std::string &response, int &term, bool &granted) const
{
    std::istringstream input(response);
    std::string tag;
    int granted_value = 0;
    if (!(input >> tag >> term >> granted_value))
    {
        return false;
    }
    if (tag != "RAFT_VOTE")
    {
        return false;
    }
    granted = granted_value == 1;
    return true;
}

bool ClusterRouter::parse_append_response(const std::string &response, int &term, bool &success) const
{
    std::istringstream input(response);
    std::string tag;
    int success_value = 0;
    if (!(input >> tag >> term >> success_value))
    {
        return false;
    }
    if (tag != "RAFT_APPEND")
    {
        return false;
    }
    success = success_value == 1;
    return true;
}

int ClusterRouter::election_timeout_ms() const
{
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> jitter(0, 600);
    size_t id_hash = std::hash<std::string>{}(m_local_id) % 300;
    return 900 + static_cast<int>(id_hash) + jitter(rng);
}

bool ClusterRouter::load_raft_meta_locked(const std::string &data_dir)
{
    std::filesystem::create_directories(data_dir);
    std::ifstream in(m_raft_meta_file);
    if (!in.is_open())
    {
        m_current_term = 0;
        m_voted_for.clear();
        return true;
    }

    std::string key;
    while (in >> key)
    {
        if (key == "term")
        {
            in >> m_current_term;
        }
        else if (key == "voted_for")
        {
            in >> m_voted_for;
            if (m_voted_for == "-")
            {
                m_voted_for.clear();
            }
        }
    }
    return true;
}

bool ClusterRouter::persist_raft_meta_locked() const
{
    if (m_raft_meta_file.empty())
    {
        return true;
    }
    std::filesystem::path path(m_raft_meta_file);
    if (path.has_parent_path())
    {
        std::filesystem::create_directories(path.parent_path());
    }
    std::ofstream out(m_raft_meta_file, std::ios::trunc);
    if (!out.is_open())
    {
        return false;
    }
    out << "term " << m_current_term << "\n";
    out << "voted_for " << (m_voted_for.empty() ? "-" : m_voted_for) << "\n";
    return static_cast<bool>(out);
}

void ClusterRouter::rebuild_hash_locked(const std::vector<ClusterNode> &alive_nodes) const
{
    m_alive_nodes = alive_nodes;
    m_hash.build(m_alive_nodes);
}

bool ClusterRouter::is_local_endpoint(const ClusterNode &node, const ServerConfig &config)
{
    return node.port == config.node.port &&
           (node.host == config.node.host ||
            node.host == "127.0.0.1" ||
            node.host == "localhost");
}
