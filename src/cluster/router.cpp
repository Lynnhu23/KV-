#include "router.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>

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
    }
    refresh_health_if_needed();
    return !m_hash.empty();
}

bool ClusterRouter::enabled() const
{
    return m_enabled;
}

bool ClusterRouter::raft_enabled() const
{
    return m_enabled && m_raft_enabled;
}

bool ClusterRouter::is_leader() const
{
    return !raft_enabled() || m_local_id == m_leader_id;
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
        if (node.id != m_leader_id)
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

    for (const auto &node : m_all_nodes)
    {
        if (node.id == m_leader_id)
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
