#include "kv_server.h"

#include "log/log.h"
#include "protocol/kv_protocol.h"
#include "storage/memory_store.h"
#include "storage/persistent_memory_store.h"

#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <optional>
#include <thread>

namespace
{
constexpr int kMaxEvents = 256;
constexpr int kReadBufferSize = 4096;

KVServer *g_server = nullptr;

void handle_signal(int)
{
    if (g_server != nullptr)
    {
        g_server->stop();
    }
}

bool set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
    {
        return false;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool send_all(int fd, const std::string &data)
{
    size_t sent = 0;
    while (sent < data.size())
    {
        ssize_t n = send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n > 0)
        {
            sent += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR)
        {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        return false;
    }
    return true;
}

bool parse_number(const std::string &text, long &out)
{
    try
    {
        size_t parsed = 0;
        out = std::stol(text, &parsed);
        return parsed == text.size();
    }
    catch (...)
    {
        return false;
    }
}

bool read_resp_line(const std::string &buffer, size_t &pos, std::string &line)
{
    size_t end = buffer.find("\r\n", pos);
    if (end == std::string::npos)
    {
        return false;
    }
    line = buffer.substr(pos, end - pos);
    pos = end + 2;
    return true;
}

bool resp_frame_size(const std::string &buffer, size_t &frame_size, bool &malformed)
{
    malformed = false;
    frame_size = 0;

    size_t pos = 0;
    std::string line;
    if (!read_resp_line(buffer, pos, line))
    {
        return false;
    }
    if (line.empty() || line[0] != '*')
    {
        malformed = true;
        frame_size = pos;
        return true;
    }

    long count = 0;
    if (!parse_number(line.substr(1), count) || count <= 0)
    {
        malformed = true;
        frame_size = pos;
        return true;
    }

    for (long i = 0; i < count; ++i)
    {
        size_t header_start = pos;
        if (!read_resp_line(buffer, pos, line))
        {
            return false;
        }
        if (line.empty() || line[0] != '$')
        {
            malformed = true;
            frame_size = pos;
            return true;
        }

        long bulk_len = 0;
        if (!parse_number(line.substr(1), bulk_len) || bulk_len < 0)
        {
            malformed = true;
            frame_size = pos;
            return true;
        }

        size_t len = static_cast<size_t>(bulk_len);
        if (pos + len + 2 > buffer.size())
        {
            return false;
        }
        if (buffer.compare(pos + len, 2, "\r\n") != 0)
        {
            malformed = true;
            frame_size = header_start;
            return true;
        }
        pos += len + 2;
    }

    frame_size = pos;
    return true;
}

bool extract_request_frame(std::string &pending, std::string &frame)
{
    if (pending.empty())
    {
        return false;
    }

    if (pending.front() != '*')
    {
        size_t newline_pos = pending.find('\n');
        if (newline_pos == std::string::npos)
        {
            return false;
        }
        frame = pending.substr(0, newline_pos);
        pending.erase(0, newline_pos + 1);
        return true;
    }

    size_t frame_size = 0;
    bool malformed = false;
    if (!resp_frame_size(pending, frame_size, malformed))
    {
        return false;
    }
    if (malformed && frame_size == 0)
    {
        frame_size = pending.size();
    }

    frame = pending.substr(0, frame_size);
    pending.erase(0, frame_size);
    return true;
}

std::string resp_simple(const std::string &value)
{
    return "+" + value + "\r\n";
}

std::string resp_error(const std::string &value)
{
    std::string message = value;
    const std::string prefix = "ERROR ";
    if (message.rfind(prefix, 0) == 0)
    {
        message.replace(0, prefix.size(), "ERR ");
    }
    return "-" + message + "\r\n";
}

std::string resp_integer(long value)
{
    return ":" + std::to_string(value) + "\r\n";
}

std::string resp_bulk(const std::optional<std::string> &value)
{
    if (!value.has_value())
    {
        return "$-1\r\n";
    }
    return "$" + std::to_string(value->size()) + "\r\n" + *value + "\r\n";
}

std::string text_response(const std::string &value)
{
    return KVProtocol::encode_response({value});
}
}

KVServer::KVServer()
    : m_listenfd(-1), m_epollfd(-1), m_running(false)
{
    g_server = this;
}

KVServer::~KVServer()
{
    stop();
    if (g_server == this)
    {
        g_server = nullptr;
    }
}

bool KVServer::init(int argc, char *argv[])
{
    if (!init_config(argc, argv) || !init_log() || !init_store() ||
        !init_cluster() || !init_thread_pool())
    {
        return false;
    }

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    std::signal(SIGPIPE, SIG_IGN);

    return init_listener() && init_epoll();
}

bool KVServer::init_config(int argc, char *argv[])
{
    m_config.parse_arg(argc, argv);
    return true;
}

bool KVServer::init_log()
{
    const auto &log_cfg = m_config.server_config.log;
    int max_queue_size = 0;
    if (log_cfg.write_mode == "async")
    {
        max_queue_size = log_cfg.max_queue_size;
    }

    return Log::get_instance()->init(log_cfg.file.c_str(),
                                     log_cfg.close ? 1 : 0,
                                     log_cfg.buffer_size,
                                     log_cfg.split_lines,
                                     max_queue_size);
}

bool KVServer::init_store()
{
    const auto &store_cfg = m_config.server_config.store;
    if (store_cfg.engine == "memory")
    {
        if (store_cfg.wal_enabled)
        {
            auto store = std::make_unique<PersistentMemoryStore>();
            if (!store->init(store_cfg.wal_file,
                             store_cfg.snapshot_file,
                             static_cast<size_t>(store_cfg.snapshot_threshold)))
            {
                LOG_ERROR("Failed to initialize WAL/snapshot: wal=%s snapshot=%s",
                          store_cfg.wal_file.c_str(), store_cfg.snapshot_file.c_str());
                return false;
            }

            m_store = std::move(store);
            LOG_INFO("KV store initialized: engine=%s wal=%s snapshot=%s threshold=%d",
                     store_cfg.engine.c_str(),
                     store_cfg.wal_file.c_str(),
                     store_cfg.snapshot_file.c_str(),
                     store_cfg.snapshot_threshold);
            return true;
        }

        m_store = std::make_unique<MemoryStore>();
        LOG_INFO("KV store initialized: engine=%s", store_cfg.engine.c_str());
        return true;
    }

    LOG_ERROR("Unsupported store engine: %s", store_cfg.engine.c_str());
    return false;
}

bool KVServer::init_cluster()
{
    if (!m_router.init(m_config.server_config))
    {
        LOG_ERROR("Failed to initialize cluster router");
        return false;
    }

    if (m_router.enabled())
    {
        LOG_INFO("Cluster routing enabled");
    }
    return true;
}

bool KVServer::init_listener()
{
    const auto &node_cfg = m_config.server_config.node;

    m_listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (m_listenfd < 0)
    {
        LOG_ERROR("socket failed: %s", strerror(errno));
        return false;
    }

    int reuse = 1;
    setsockopt(m_listenfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    if (!set_nonblocking(m_listenfd))
    {
        LOG_ERROR("set nonblocking failed: %s", strerror(errno));
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    if (node_cfg.host == "0.0.0.0")
    {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    else if (inet_pton(AF_INET, node_cfg.host.c_str(), &addr.sin_addr) != 1)
    {
        LOG_ERROR("Invalid node host: %s", node_cfg.host.c_str());
        return false;
    }
    addr.sin_port = htons(node_cfg.port);

    if (bind(m_listenfd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        LOG_ERROR("bind failed on %s:%d: %s",
                  node_cfg.host.c_str(), node_cfg.port, strerror(errno));
        return false;
    }

    if (listen(m_listenfd, 128) < 0)
    {
        LOG_ERROR("listen failed: %s", strerror(errno));
        return false;
    }

    LOG_INFO("KV node %s listening on %s:%d",
             node_cfg.id.c_str(), node_cfg.host.c_str(), node_cfg.port);
    return true;
}

bool KVServer::init_thread_pool()
{
    m_thread_pool = std::make_unique<ThreadPool>();
    if (!m_thread_pool->start())
    {
        LOG_ERROR("Failed to start worker thread pool");
        return false;
    }
    LOG_INFO("Worker thread pool started: threads=%zu", m_thread_pool->size());
    return true;
}

bool KVServer::init_epoll()
{
    m_epollfd = epoll_create1(EPOLL_CLOEXEC);
    if (m_epollfd < 0)
    {
        LOG_ERROR("epoll_create1 failed: %s", strerror(errno));
        return false;
    }

    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = m_listenfd;
    if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, m_listenfd, &event) < 0)
    {
        LOG_ERROR("epoll_ctl listen fd failed: %s", strerror(errno));
        return false;
    }
    return true;
}

int KVServer::start()
{
    m_running = true;
    event_loop();
    return 0;
}

void KVServer::stop()
{
    m_running = false;
    if (m_epollfd >= 0)
    {
        close(m_epollfd);
        m_epollfd = -1;
    }
    if (m_listenfd >= 0)
    {
        close(m_listenfd);
        m_listenfd = -1;
    }
    if (m_thread_pool)
    {
        m_thread_pool->stop();
    }
}

void KVServer::event_loop()
{
    epoll_event events[kMaxEvents];
    while (m_running)
    {
        int n = epoll_wait(m_epollfd, events, kMaxEvents, 1000);
        if (n < 0)
        {
            if (!m_running || errno == EINTR)
            {
                continue;
            }
            LOG_ERROR("epoll_wait failed: %s", strerror(errno));
            continue;
        }

        for (int i = 0; i < n; ++i)
        {
            int fd = events[i].data.fd;
            if (fd == m_listenfd)
            {
                accept_clients();
                continue;
            }

            if ((events[i].events & EPOLLIN) != 0)
            {
                handle_client_read(fd);
                continue;
            }

            if ((events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0)
            {
                close_client(fd);
            }
        }
    }
}

void KVServer::accept_clients()
{
    while (true)
    {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(m_listenfd,
                               reinterpret_cast<sockaddr *>(&client_addr),
                               &client_len);
        if (client_fd < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return;
            }
            if (errno != EINTR)
            {
                LOG_ERROR("accept failed: %s", strerror(errno));
            }
            continue;
        }

        if (!set_nonblocking(client_fd))
        {
            LOG_ERROR("set client nonblocking failed: %s", strerror(errno));
            close(client_fd);
            continue;
        }

        epoll_event event{};
        event.events = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT;
        event.data.fd = client_fd;
        if (epoll_ctl(m_epollfd, EPOLL_CTL_ADD, client_fd, &event) < 0)
        {
            LOG_ERROR("epoll_ctl client fd failed: %s", strerror(errno));
            close(client_fd);
            continue;
        }

        {
            std::lock_guard<std::mutex> lock(m_clients_mutex);
            m_client_buffers.emplace(client_fd, std::string{});
        }
    }
}

void KVServer::handle_client_read(int client_fd)
{
    char buffer[kReadBufferSize];
    std::vector<std::string> lines;
    bool close_after_send = false;

    while (true)
    {
        ssize_t n = recv(client_fd, buffer, sizeof(buffer), 0);
        if (n == 0)
        {
            {
                std::lock_guard<std::mutex> lock(m_clients_mutex);
                auto it = m_client_buffers.find(client_fd);
                if (it != m_client_buffers.end() && !it->second.empty())
                {
                    lines.push_back(it->second);
                    it->second.clear();
                }
            }
            close_after_send = true;
            remove_client_from_epoll(client_fd);
            break;
        }
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                break;
            }
            close_client(client_fd);
            return;
        }

        std::lock_guard<std::mutex> lock(m_clients_mutex);
        auto it = m_client_buffers.find(client_fd);
        if (it == m_client_buffers.end())
        {
            return;
        }

        std::string &pending = it->second;
        pending.append(buffer, static_cast<size_t>(n));

        std::string frame;
        while (extract_request_frame(pending, frame))
        {
            lines.push_back(frame);
        }
    }

    if (!lines.empty())
    {
        if (!m_thread_pool->submit([this, client_fd, close_after_send, lines = std::move(lines)]() mutable {
                process_client_lines(client_fd, std::move(lines), close_after_send);
            }))
        {
            close_client(client_fd);
        }
        return;
    }

    if (close_after_send)
    {
        close_client(client_fd);
        return;
    }

    rearm_client(client_fd);
}

void KVServer::close_client(int client_fd)
{
    if (client_fd < 0)
    {
        return;
    }
    if (m_epollfd >= 0)
    {
        epoll_ctl(m_epollfd, EPOLL_CTL_DEL, client_fd, nullptr);
    }
    {
        std::lock_guard<std::mutex> lock(m_clients_mutex);
        m_client_buffers.erase(client_fd);
    }
    close(client_fd);
}

void KVServer::remove_client_from_epoll(int client_fd)
{
    if (m_epollfd >= 0)
    {
        epoll_ctl(m_epollfd, EPOLL_CTL_DEL, client_fd, nullptr);
    }
    std::lock_guard<std::mutex> lock(m_clients_mutex);
    m_client_buffers.erase(client_fd);
}

void KVServer::rearm_client(int client_fd)
{
    {
        std::lock_guard<std::mutex> lock(m_clients_mutex);
        if (m_client_buffers.find(client_fd) == m_client_buffers.end())
        {
            return;
        }
    }

    epoll_event event{};
    event.events = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT;
    event.data.fd = client_fd;
    if (m_epollfd >= 0 && epoll_ctl(m_epollfd, EPOLL_CTL_MOD, client_fd, &event) < 0)
    {
        close_client(client_fd);
    }
}

void KVServer::process_client_lines(int client_fd, std::vector<std::string> lines, bool close_after_send)
{
    std::string response;
    for (const auto &line : lines)
    {
        response += process_request_line(line);
    }

    if (!response.empty() && !send_all(client_fd, response))
    {
        close_client(client_fd);
        return;
    }

    if (close_after_send)
    {
        close(client_fd);
        return;
    }

    rearm_client(client_fd);
}

std::string KVServer::process_request_line(const std::string &line)
{
    Request request = KVProtocol::parse_request(line);
    if (!request.error.empty())
    {
        if (request.protocol == ProtocolType::Resp)
        {
            return resp_error(request.error);
        }
        return text_response(request.error);
    }

    if (m_router.should_route(request) && !m_router.is_local_owner(request.key))
    {
        auto owner = m_router.owner_for(request.key);
        if (!owner.has_value())
        {
            return KVProtocol::encode_response({"ERROR route_unavailable"});
        }
        return m_router.forward(*owner, line);
    }

    switch (request.type)
    {
    case CommandType::Ping:
        return request.protocol == ProtocolType::Resp ? resp_simple("PONG") : text_response("PONG");
    case CommandType::Put:
    {
        bool ok = m_store->put(request.key, request.value);
        if (request.protocol == ProtocolType::Resp)
        {
            return ok ? resp_simple("OK") : resp_error("ERR put_failed");
        }
        return text_response(ok ? "OK" : "ERROR put_failed");
    }
    case CommandType::Get:
    {
        auto value = m_store->get(request.key);
        if (request.protocol == ProtocolType::Resp)
        {
            return resp_bulk(value);
        }
        if (!value.has_value())
        {
            return text_response("NOT_FOUND");
        }
        return text_response("VALUE " + *value);
    }
    case CommandType::Del:
    {
        bool removed = m_store->del(request.key);
        return request.protocol == ProtocolType::Resp
                   ? resp_integer(removed ? 1 : 0)
                   : text_response(removed ? "OK" : "NOT_FOUND");
    }
    case CommandType::Exists:
    {
        bool exists = m_store->exists(request.key);
        return request.protocol == ProtocolType::Resp
                   ? resp_integer(exists ? 1 : 0)
                   : text_response(exists ? "TRUE" : "FALSE");
    }
    case CommandType::Unknown:
        break;
    }

    return "";
}
