#include "router.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

bool ClusterRouter::init(const ServerConfig &config)
{
    m_enabled = config.cluster.enabled;
    m_local_id = config.node.id;

    if (!m_enabled)
    {
        return true;
    }

    std::vector<ClusterNode> nodes;
    for (const auto &peer : config.cluster.peers)
    {
        nodes.push_back({peer.id, peer.host, peer.port});
    }

    bool has_local = false;
    for (const auto &node : nodes)
    {
        if (node.id == config.node.id || is_local_endpoint(node, config))
        {
            has_local = true;
            break;
        }
    }

    if (!has_local)
    {
        nodes.push_back({config.node.id, "127.0.0.1", config.node.port});
    }

    m_hash.build(nodes);
    return !m_hash.empty();
}

bool ClusterRouter::enabled() const
{
    return m_enabled;
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
           request.type == CommandType::Exists;
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
    return m_hash.owner_for(key);
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

bool ClusterRouter::is_local_endpoint(const ClusterNode &node, const ServerConfig &config)
{
    return node.port == config.node.port &&
           (node.host == config.node.host ||
            node.host == "127.0.0.1" ||
            node.host == "localhost");
}
