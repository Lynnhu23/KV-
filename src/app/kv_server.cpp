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

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <optional>
#include <sstream>
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

std::string strip_line_end(std::string value)
{
    while (!value.empty() && (value.back() == '\n' || value.back() == '\r'))
    {
        value.pop_back();
    }
    return value;
}

std::optional<std::string> parse_text_value(const std::string &response)
{
    const std::string prefix = "VALUE ";
    if (response.rfind(prefix, 0) != 0)
    {
        return std::nullopt;
    }
    return strip_line_end(response.substr(prefix.size()));
}

bool response_ok(const std::string &response)
{
    return response == "OK\n" || response == "+OK\r\n";
}

bool is_client_write(CommandType type)
{
    return type == CommandType::Put ||
           type == CommandType::Del ||
           type == CommandType::Expire;
}

std::string raft_command_for(const Request &request)
{
    switch (request.type)
    {
    case CommandType::Put:
        return "RAFT_PUT " + request.key + " " + request.value + "\n";
    case CommandType::Del:
        return "RAFT_DEL " + request.key + "\n";
    case CommandType::Expire:
        return "RAFT_EXPIRE " + request.key + " " + std::to_string(request.ttl_seconds) + "\n";
    default:
        return "";
    }
}

std::string hex_encode(const std::string &value)
{
    if (value.empty())
    {
        return "-";
    }

    std::ostringstream output;
    output << std::hex << std::setfill('0');
    for (unsigned char ch : value)
    {
        output << std::setw(2) << static_cast<int>(ch);
    }
    return output.str();
}

bool parse_raft_append_response(const std::string &response, int &term, bool &success, int &match_index)
{
    std::istringstream input(response);
    std::string tag;
    int success_value = 0;
    if (!(input >> tag >> term >> success_value >> match_index))
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

bool parse_raft_snapshot_response(const std::string &response, int &term, bool &success)
{
    std::istringstream input(response);
    std::string tag;
    int success_value = 0;
    if (!(input >> tag >> term >> success_value))
    {
        return false;
    }
    if (tag != "RAFT_SNAPSHOT")
    {
        return false;
    }
    success = success_value == 1;
    return true;
}

int local_hex_value(char ch)
{
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
}

bool hex_decode(const std::string &encoded, std::string &decoded)
{
    if (encoded == "-")
    {
        decoded.clear();
        return true;
    }
    if (encoded.size() % 2 != 0)
    {
        return false;
    }
    decoded.clear();
    decoded.reserve(encoded.size() / 2);
    for (size_t i = 0; i < encoded.size(); i += 2)
    {
        int high = local_hex_value(encoded[i]);
        int low = local_hex_value(encoded[i + 1]);
        if (high < 0 || low < 0)
        {
            return false;
        }
        decoded.push_back(static_cast<char>((high << 4) | low));
    }
    return true;
}
}

KVServer::KVServer()
    : m_listenfd(-1), m_epollfd(-1), m_running(false)
{
    m_metrics_started_at = std::chrono::steady_clock::now();
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
        !init_cluster() || !init_raft_persistence() || !init_thread_pool())
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
            store->set_max_keys(static_cast<size_t>(store_cfg.max_keys));

            m_store = std::move(store);
            LOG_INFO("KV store initialized: engine=%s wal=%s snapshot=%s threshold=%d max_keys=%d",
                     store_cfg.engine.c_str(),
                     store_cfg.wal_file.c_str(),
                     store_cfg.snapshot_file.c_str(),
                     store_cfg.snapshot_threshold,
                     store_cfg.max_keys);
            return true;
        }

        m_store = std::make_unique<MemoryStore>();
        m_store->set_max_keys(static_cast<size_t>(store_cfg.max_keys));
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
    m_router.stop();
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
        auto begin = std::chrono::steady_clock::now();
        std::string line_response = process_request_line(line);
        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::steady_clock::now() - begin)
                              .count();
        bool failed = line_response.rfind("ERROR", 0) == 0 ||
                      line_response.rfind("-ERR", 0) == 0 ||
                      line_response.rfind("-ERROR", 0) == 0;
        record_request_metrics(elapsed_us, failed);
        response += line_response;
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

void KVServer::record_request_metrics(long long latency_us, bool failed)
{
    m_requests_total.fetch_add(1, std::memory_order_relaxed);
    if (failed)
    {
        m_failed_requests_total.fetch_add(1, std::memory_order_relaxed);
    }

    std::lock_guard<std::mutex> lock(m_latency_mutex);
    m_recent_latencies_us.push_back(latency_us);
    constexpr size_t kMaxRecentLatencies = 4096;
    if (m_recent_latencies_us.size() > kMaxRecentLatencies)
    {
        m_recent_latencies_us.erase(m_recent_latencies_us.begin(),
                                    m_recent_latencies_us.begin() +
                                        static_cast<long long>(m_recent_latencies_us.size() - kMaxRecentLatencies));
    }
}

std::string KVServer::metrics_response()
{
    std::vector<long long> latencies;
    {
        std::lock_guard<std::mutex> lock(m_latency_mutex);
        latencies = m_recent_latencies_us;
    }
    std::sort(latencies.begin(), latencies.end());

    auto percentile = [&](double p) -> long long {
        if (latencies.empty())
        {
            return 0;
        }
        size_t index = static_cast<size_t>((latencies.size() - 1) * p);
        return latencies[index];
    };

    int commit_index = 0;
    int last_applied = 0;
    int last_log = 0;
    int last_included_index = 0;
    int last_included_term = 0;
    size_t log_size = 0;
    {
        std::lock_guard<std::mutex> lock(m_raft_log_mutex);
        commit_index = m_raft_commit_index;
        last_applied = m_raft_last_applied;
        last_log = raft_last_log_index_locked();
        last_included_index = m_raft_last_included_index;
        last_included_term = m_raft_last_included_term;
        log_size = m_raft_log.size();
    }

    auto requests = m_requests_total.load(std::memory_order_relaxed);
    auto failed = m_failed_requests_total.load(std::memory_order_relaxed);
    auto replication_failures = m_replication_failures_total.load(std::memory_order_relaxed);
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - m_metrics_started_at).count();
    if (elapsed <= 0)
    {
        elapsed = 1;
    }

    std::ostringstream output;
    size_t alive_nodes = m_router.enabled() ? m_router.alive_nodes().size() : 1;
    output << "tinykv_node_id " << m_router.local_id() << "\n";
    output << "tinykv_raft_role " << (m_router.raft_enabled() ? m_router.role_name() : "standalone") << "\n";
    output << "tinykv_raft_term " << m_router.current_term() << "\n";
    output << "tinykv_raft_leader " << m_router.leader_id() << "\n";
    output << "tinykv_raft_commit_index " << commit_index << "\n";
    output << "tinykv_raft_last_applied " << last_applied << "\n";
    output << "tinykv_raft_log_size " << log_size << "\n";
    output << "tinykv_raft_last_log_index " << last_log << "\n";
    output << "tinykv_raft_snapshot_last_included_index " << last_included_index << "\n";
    output << "tinykv_raft_snapshot_last_included_term " << last_included_term << "\n";
    output << "tinykv_raft_election_count " << m_router.election_count() << "\n";
    output << "tinykv_raft_replication_failure_count " << replication_failures << "\n";
    output << "tinykv_alive_nodes " << alive_nodes << "\n";
    output << "tinykv_store_keys " << m_store->size() << "\n";
    output << "tinykv_requests_total " << requests << "\n";
    output << "tinykv_failed_requests_total " << failed << "\n";
    output << "tinykv_qps " << static_cast<unsigned long long>(requests / elapsed) << "\n";
    output << "tinykv_latency_p95_us " << percentile(0.95) << "\n";
    output << "tinykv_latency_p99_us " << percentile(0.99) << "\n";
    return output.str();
}

int KVServer::raft_last_log_index_locked() const
{
    return m_raft_log.empty() ? m_raft_last_included_index : m_raft_log.back().index;
}

int KVServer::raft_last_log_term_locked() const
{
    return m_raft_log.empty() ? m_raft_last_included_term : m_raft_log.back().term;
}

int KVServer::raft_log_offset_locked(int index) const
{
    int offset = index - m_raft_last_included_index - 1;
    if (offset < 0 || static_cast<size_t>(offset) >= m_raft_log.size())
    {
        return -1;
    }
    return offset;
}

int KVServer::raft_term_at_locked(int index) const
{
    if (index <= 0)
    {
        return 0;
    }
    if (index == m_raft_last_included_index)
    {
        return m_raft_last_included_term;
    }
    int offset = raft_log_offset_locked(index);
    if (offset < 0)
    {
        return -1;
    }
    return m_raft_log[static_cast<size_t>(offset)].term;
}

bool KVServer::init_raft_persistence()
{
    const auto &node_cfg = m_config.server_config.node;
    std::filesystem::create_directories(node_cfg.data_dir);
    m_raft_log_file = node_cfg.data_dir + "/raft.log";
    m_raft_state_file = node_cfg.data_dir + "/raft.state";
    m_raft_snapshot_threshold = std::max(1, m_config.server_config.store.snapshot_threshold);
    if (!load_raft_state())
    {
        return false;
    }
    std::lock_guard<std::mutex> lock(m_raft_log_mutex);
    m_router.update_log_info(raft_last_log_index_locked(), raft_last_log_term_locked());
    apply_committed_raft_entries_locked();
    return true;
}

bool KVServer::load_raft_state()
{
    std::lock_guard<std::mutex> lock(m_raft_log_mutex);
    m_raft_log.clear();
    m_raft_commit_index = 0;
    m_raft_last_applied = 0;
    m_raft_last_included_index = 0;
    m_raft_last_included_term = 0;

    {
        std::ifstream state(m_raft_state_file);
        std::string key;
        while (state >> key)
        {
            if (key == "commit_index")
            {
                state >> m_raft_commit_index;
            }
            else if (key == "last_applied")
            {
                state >> m_raft_last_applied;
            }
            else if (key == "last_included_index")
            {
                state >> m_raft_last_included_index;
            }
            else if (key == "last_included_term")
            {
                state >> m_raft_last_included_term;
            }
        }
        m_raft_last_applied = std::min(m_raft_last_applied, m_raft_commit_index);
    }

    std::ifstream log(m_raft_log_file);
    if (!log.is_open())
    {
        return true;
    }

    std::string line;
    while (std::getline(log, line))
    {
        if (line.empty())
        {
            continue;
        }
        std::istringstream input(line);
        int index = 0;
        int term = 0;
        std::string op;
        std::string key_hex;
        int ttl = 0;
        std::string value_hex;
        if (!(input >> index >> term >> op >> key_hex >> ttl >> value_hex))
        {
            return false;
        }
        std::string key;
        std::string value;
        if (!hex_decode(key_hex, key) || !hex_decode(value_hex, value))
        {
            return false;
        }
        CommandType type = CommandType::Unknown;
        if (op == "PUT") type = CommandType::Put;
        if (op == "DEL") type = CommandType::Del;
        if (op == "EXPIRE") type = CommandType::Expire;
        if (type == CommandType::Unknown)
        {
            return false;
        }
        m_raft_log.push_back({index, term, type, key, value, ttl});
    }
    m_raft_commit_index = std::max(m_raft_last_included_index,
                                   std::min(m_raft_commit_index, raft_last_log_index_locked()));
    m_raft_last_applied = std::max(m_raft_last_included_index,
                                   std::min(m_raft_last_applied, m_raft_commit_index));
    return true;
}

bool KVServer::persist_raft_state_locked() const
{
    if (m_raft_state_file.empty())
    {
        return true;
    }
    std::filesystem::path path(m_raft_state_file);
    if (path.has_parent_path())
    {
        std::filesystem::create_directories(path.parent_path());
    }
    std::ofstream out(m_raft_state_file, std::ios::trunc);
    if (!out.is_open())
    {
        return false;
    }
    out << "commit_index " << m_raft_commit_index << "\n";
    out << "last_applied " << m_raft_last_applied << "\n";
    out << "last_included_index " << m_raft_last_included_index << "\n";
    out << "last_included_term " << m_raft_last_included_term << "\n";
    return static_cast<bool>(out);
}

bool KVServer::persist_raft_log_locked() const
{
    if (m_raft_log_file.empty())
    {
        return true;
    }
    std::filesystem::path path(m_raft_log_file);
    if (path.has_parent_path())
    {
        std::filesystem::create_directories(path.parent_path());
    }
    std::ofstream out(m_raft_log_file, std::ios::trunc);
    if (!out.is_open())
    {
        return false;
    }
    for (const auto &entry : m_raft_log)
    {
        std::string op = entry.type == CommandType::Put ? "PUT" :
                         entry.type == CommandType::Del ? "DEL" :
                         entry.type == CommandType::Expire ? "EXPIRE" : "UNKNOWN";
        out << entry.index << ' ' << entry.term << ' ' << op << ' '
            << hex_encode(entry.key) << ' ' << entry.ttl_seconds << ' '
            << hex_encode(entry.value) << '\n';
    }
    return static_cast<bool>(out);
}

bool KVServer::persist_raft_locked() const
{
    return persist_raft_log_locked() && persist_raft_state_locked();
}

bool KVServer::apply_raft_entry_locked(const RaftLogEntry &entry)
{
    switch (entry.type)
    {
    case CommandType::Put:
        return entry.ttl_seconds > 0
                   ? m_store->put_ttl(entry.key, entry.value, entry.ttl_seconds)
                   : m_store->put(entry.key, entry.value);
    case CommandType::Del:
        m_store->del(entry.key);
        return true;
    case CommandType::Expire:
        m_store->expire(entry.key, entry.ttl_seconds);
        return true;
    default:
        return false;
    }
}

void KVServer::apply_committed_raft_entries_locked()
{
    if (m_raft_last_applied < m_raft_last_included_index)
    {
        m_raft_last_applied = m_raft_last_included_index;
    }
    while (m_raft_last_applied < m_raft_commit_index)
    {
        int next_index = m_raft_last_applied + 1;
        int offset = raft_log_offset_locked(next_index);
        if (offset < 0)
        {
            break;
        }
        RaftLogEntry &entry = m_raft_log[static_cast<size_t>(offset)];
        apply_raft_entry_locked(entry);
        m_raft_last_applied = entry.index;
    }
    compact_raft_log_if_needed_locked();
    persist_raft_state_locked();
}

void KVServer::compact_raft_log_if_needed_locked()
{
    int compact_to = m_raft_last_applied;
    if (compact_to <= m_raft_last_included_index)
    {
        return;
    }
    if (compact_to - m_raft_last_included_index < m_raft_snapshot_threshold)
    {
        return;
    }

    int compact_term = raft_term_at_locked(compact_to);
    if (compact_term < 0)
    {
        return;
    }

    auto remove_end = std::remove_if(m_raft_log.begin(), m_raft_log.end(), [&](const RaftLogEntry &entry) {
        return entry.index <= compact_to;
    });
    m_raft_log.erase(remove_end, m_raft_log.end());
    m_raft_last_included_index = compact_to;
    m_raft_last_included_term = compact_term;
    persist_raft_locked();
    m_router.update_log_info(raft_last_log_index_locked(), raft_last_log_term_locked());
}

std::string KVServer::encode_raft_append(const RaftLogEntry &entry,
                                         int prev_index,
                                         int prev_term,
                                         int leader_commit) const
{
    std::string op = "NOOP";
    if (entry.type == CommandType::Put)
    {
        op = "PUT";
    }
    else if (entry.type == CommandType::Del)
    {
        op = "DEL";
    }
    else if (entry.type == CommandType::Expire)
    {
        op = "EXPIRE";
    }

    return "RAFT_APPEND_ENTRIES " + std::to_string(entry.term) + " " + m_router.local_id() + " " +
           std::to_string(prev_index) + " " + std::to_string(prev_term) + " " +
           std::to_string(leader_commit) + " " + std::to_string(entry.index) + " " +
           std::to_string(entry.term) + " " + op + " " + hex_encode(entry.key) + " " +
           std::to_string(entry.ttl_seconds) + " " + hex_encode(entry.value) + "\n";
}

std::string KVServer::encode_raft_append_batch(const std::vector<RaftLogEntry> &entries,
                                               int prev_index,
                                               int prev_term,
                                               int leader_commit) const
{
    std::ostringstream payload;
    for (const auto &entry : entries)
    {
        std::string op = entry.type == CommandType::Put ? "PUT" :
                         entry.type == CommandType::Del ? "DEL" :
                         entry.type == CommandType::Expire ? "EXPIRE" : "UNKNOWN";
        payload << entry.index << ' ' << entry.term << ' ' << op << ' '
                << hex_encode(entry.key) << ' ' << entry.ttl_seconds << ' '
                << hex_encode(entry.value) << '\n';
    }
    int term = entries.empty() ? m_router.current_term() : entries.back().term;
    return "RAFT_APPEND_ENTRIES " + std::to_string(term) + " " + m_router.local_id() + " " +
           std::to_string(prev_index) + " " + std::to_string(prev_term) + " " +
           std::to_string(leader_commit) + " " + std::to_string(entries.size()) + " " +
           hex_encode(payload.str()) + "\n";
}

std::string KVServer::encode_raft_commit(int term, int leader_commit, int prev_index, int prev_term) const
{
    return "RAFT_APPEND_ENTRIES " + std::to_string(term) + " " + m_router.local_id() + " " +
           std::to_string(prev_index) + " " + std::to_string(prev_term) + " " +
           std::to_string(leader_commit) + " 0 " + hex_encode("") + "\n";
}

std::vector<KVServer::RaftLogEntry> KVServer::decode_raft_entries_payload(const std::string &payload) const
{
    std::vector<RaftLogEntry> entries;
    std::istringstream lines(payload);
    std::string line;
    while (std::getline(lines, line))
    {
        if (line.empty())
        {
            continue;
        }
        std::istringstream input(line);
        int index = 0;
        int term = 0;
        std::string op;
        std::string key_hex;
        int ttl = 0;
        std::string value_hex;
        if (!(input >> index >> term >> op >> key_hex >> ttl >> value_hex))
        {
            return {};
        }
        std::string key;
        std::string value;
        if (!hex_decode(key_hex, key) || !hex_decode(value_hex, value))
        {
            return {};
        }
        CommandType type = CommandType::Unknown;
        if (op == "PUT") type = CommandType::Put;
        if (op == "DEL") type = CommandType::Del;
        if (op == "EXPIRE") type = CommandType::Expire;
        if (type == CommandType::Unknown)
        {
            return {};
        }
        entries.push_back({index, term, type, key, value, ttl});
    }
    return entries;
}

std::string KVServer::encode_snapshot_payload() const
{
    std::ostringstream payload;
    for (const auto &entry : m_store->snapshot_entries())
    {
        payload << hex_encode(entry.key) << ' ' << hex_encode(entry.value) << ' '
                << entry.expire_at_ms << '\n';
    }
    return payload.str();
}

std::string KVServer::encode_raft_snapshot(int term)
{
    std::lock_guard<std::mutex> lock(m_raft_log_mutex);
    return "RAFT_INSTALL_SNAPSHOT " + std::to_string(term) + " " + m_router.local_id() + " " +
           std::to_string(m_raft_last_included_index) + " " +
           std::to_string(m_raft_last_included_term) + " " +
           hex_encode(encode_snapshot_payload()) + "\n";
}

bool KVServer::install_snapshot_payload(const std::string &payload)
{
    std::vector<StoreEntry> entries;
    std::istringstream lines(payload);
    std::string line;
    while (std::getline(lines, line))
    {
        if (line.empty())
        {
            continue;
        }
        std::istringstream input(line);
        std::string key_hex;
        std::string value_hex;
        long long expire_at_ms = 0;
        if (!(input >> key_hex >> value_hex >> expire_at_ms))
        {
            return false;
        }
        std::string key;
        std::string value;
        if (!hex_decode(key_hex, key) || !hex_decode(value_hex, value))
        {
            return false;
        }
        entries.push_back({key, value, expire_at_ms});
    }
    return m_store->replace_with_snapshot(entries);
}

std::string KVServer::handle_raft_append_entries(const Request &request)
{
    std::string heartbeat = m_router.handle_append_entries(request.term, request.node_id);
    if (heartbeat.rfind("RAFT_APPEND ", 0) != 0 || heartbeat.find(" 0") != std::string::npos)
    {
        std::lock_guard<std::mutex> lock(m_raft_log_mutex);
        return "RAFT_APPEND " + std::to_string(m_router.current_term()) + " 0 " +
               std::to_string(raft_last_log_index_locked()) + "\n";
    }

    std::lock_guard<std::mutex> lock(m_raft_log_mutex);
    int last_index = raft_last_log_index_locked();
    if (request.prev_log_index < m_raft_last_included_index)
    {
        return "RAFT_APPEND " + std::to_string(m_router.current_term()) + " 0 " +
               std::to_string(m_raft_last_included_index) + "\n";
    }
    if (request.prev_log_index > last_index)
    {
        return "RAFT_APPEND " + std::to_string(m_router.current_term()) + " 0 " +
               std::to_string(last_index) + "\n";
    }

    if (request.prev_log_index > 0 && raft_term_at_locked(request.prev_log_index) != request.prev_log_term)
    {
        int erase_offset = raft_log_offset_locked(request.prev_log_index);
        if (erase_offset >= 0)
        {
            m_raft_log.erase(m_raft_log.begin() + erase_offset, m_raft_log.end());
        }
        if (m_raft_commit_index >= request.prev_log_index)
        {
            m_raft_commit_index = request.prev_log_index - 1;
        }
        if (m_raft_last_applied >= request.prev_log_index)
        {
            m_raft_last_applied = request.prev_log_index - 1;
        }
        persist_raft_locked();
        m_router.update_log_info(raft_last_log_index_locked(), raft_last_log_term_locked());
        return "RAFT_APPEND " + std::to_string(m_router.current_term()) + " 0 " +
               std::to_string(raft_last_log_index_locked()) + "\n";
    }

    if (request.entry_count > 0)
    {
        auto entries = decode_raft_entries_payload(request.snapshot_payload);
        if (entries.size() != static_cast<size_t>(request.entry_count))
        {
            return "RAFT_APPEND " + std::to_string(m_router.current_term()) + " 0 " +
                   std::to_string(raft_last_log_index_locked()) + "\n";
        }
        int expected_index = request.prev_log_index + 1;
        for (const auto &entry : entries)
        {
            if (entry.index != expected_index)
            {
                return "RAFT_APPEND " + std::to_string(m_router.current_term()) + " 0 " +
                       std::to_string(raft_last_log_index_locked()) + "\n";
            }
            if (entry.index > raft_last_log_index_locked() + 1)
            {
                return "RAFT_APPEND " + std::to_string(m_router.current_term()) + " 0 " +
                       std::to_string(raft_last_log_index_locked()) + "\n";
            }
            int existing_term = raft_term_at_locked(entry.index);
            if (existing_term != -1 && existing_term != entry.term)
            {
                int erase_offset = raft_log_offset_locked(entry.index);
                if (erase_offset >= 0)
                {
                    m_raft_log.erase(m_raft_log.begin() + erase_offset, m_raft_log.end());
                }
            }
            if (entry.index == raft_last_log_index_locked() + 1)
            {
                m_raft_log.push_back(entry);
            }
            ++expected_index;
        }
        persist_raft_log_locked();
        m_router.update_log_info(raft_last_log_index_locked(), raft_last_log_term_locked());
    }
    else if (request.log_index > 0)
    {
        if (request.log_index > raft_last_log_index_locked() + 1)
        {
            return "RAFT_APPEND " + std::to_string(m_router.current_term()) + " 0 " +
                   std::to_string(raft_last_log_index_locked()) + "\n";
        }

        int existing_term = raft_term_at_locked(request.log_index);
        if (existing_term != -1 && existing_term != request.log_term)
        {
            int erase_offset = raft_log_offset_locked(request.log_index);
            if (erase_offset >= 0)
            {
                m_raft_log.erase(m_raft_log.begin() + erase_offset, m_raft_log.end());
            }
            persist_raft_locked();
        }

        if (request.log_index == raft_last_log_index_locked() + 1)
        {
            CommandType entry_type = CommandType::Unknown;
            if (request.raft_op == "PUT")
            {
                entry_type = CommandType::Put;
            }
            else if (request.raft_op == "DEL")
            {
                entry_type = CommandType::Del;
            }
            else if (request.raft_op == "EXPIRE")
            {
                entry_type = CommandType::Expire;
            }

            if (entry_type != CommandType::Unknown)
            {
                m_raft_log.push_back({request.log_index,
                                      request.log_term,
                                      entry_type,
                                      request.key,
                                      request.value,
                                      request.ttl_seconds});
                persist_raft_log_locked();
                m_router.update_log_info(raft_last_log_index_locked(), raft_last_log_term_locked());
            }
        }
    }

    if (request.leader_commit > m_raft_commit_index)
    {
        m_raft_commit_index = std::min(request.leader_commit, raft_last_log_index_locked());
        apply_committed_raft_entries_locked();
    }

    return "RAFT_APPEND " + std::to_string(m_router.current_term()) + " 1 " +
           std::to_string(raft_last_log_index_locked()) + "\n";
}

std::string KVServer::replicate_raft_write(const Request &request)
{
    RaftLogEntry entry;
    std::vector<RaftLogEntry> log_snapshot;
    int commit_snapshot = 0;
    int snapshot_index = 0;
    int snapshot_term = 0;
    {
        std::lock_guard<std::mutex> lock(m_raft_log_mutex);
        entry.index = raft_last_log_index_locked() + 1;
        entry.term = m_router.current_term();
        entry.type = request.type;
        entry.key = request.key;
        entry.value = request.value;
        entry.ttl_seconds = request.ttl_seconds;
        m_raft_log.push_back(entry);
        persist_raft_log_locked();
        m_router.update_log_info(raft_last_log_index_locked(), raft_last_log_term_locked());
        log_snapshot = m_raft_log;
        commit_snapshot = m_raft_commit_index;
        snapshot_index = m_raft_last_included_index;
        snapshot_term = m_raft_last_included_term;
    }

    int acks = 1;
    for (const auto &follower : m_router.raft_followers())
    {
        int next_index = entry.index;
        bool replicated = false;
        while (next_index <= entry.index && next_index > 0)
        {
            if (next_index <= snapshot_index)
            {
                int response_term = 0;
                bool snapshot_success = false;
                std::string response = m_router.forward(follower, encode_raft_snapshot(entry.term));
                if (!parse_raft_snapshot_response(response, response_term, snapshot_success))
                {
                    m_replication_failures_total.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                if (response_term > entry.term)
                {
                    m_replication_failures_total.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                if (!snapshot_success)
                {
                    m_replication_failures_total.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                next_index = snapshot_index + 1;
                continue;
            }

            int prev_index = next_index - 1;
            int prev_term = 0;
            if (prev_index == snapshot_index)
            {
                prev_term = snapshot_term;
            }
            else if (prev_index > snapshot_index)
            {
                size_t prev_offset = static_cast<size_t>(prev_index - snapshot_index - 1);
                if (prev_offset >= log_snapshot.size())
                {
                    m_replication_failures_total.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                prev_term = log_snapshot[prev_offset].term;
            }
            std::vector<RaftLogEntry> to_send;
            for (int index = next_index; index <= entry.index; ++index)
            {
                size_t offset = static_cast<size_t>(index - snapshot_index - 1);
                if (offset >= log_snapshot.size())
                {
                    to_send.clear();
                    break;
                }
                to_send.push_back(log_snapshot[offset]);
            }
            if (to_send.empty())
            {
                m_replication_failures_total.fetch_add(1, std::memory_order_relaxed);
                break;
            }
            int response_term = 0;
            bool success = false;
            int match_index = 0;
            std::string response = m_router.forward(follower,
                                                    encode_raft_append_batch(to_send, prev_index, prev_term, commit_snapshot));
            if (!parse_raft_append_response(response, response_term, success, match_index))
            {
                m_replication_failures_total.fetch_add(1, std::memory_order_relaxed);
                break;
            }
            if (response_term > entry.term)
            {
                m_replication_failures_total.fetch_add(1, std::memory_order_relaxed);
                replicated = false;
                break;
            }
            if (success)
            {
                if (match_index >= entry.index)
                {
                    replicated = true;
                    break;
                }
                next_index = match_index + 1;
                continue;
            }
            if (match_index < next_index - 1)
            {
                next_index = match_index + 1;
                continue;
            }
            m_replication_failures_total.fetch_add(1, std::memory_order_relaxed);
            break;
        }
        if (replicated)
        {
            ++acks;
        }
    }

    if (acks < m_router.majority_size())
    {
        m_replication_failures_total.fetch_add(1, std::memory_order_relaxed);
        return request.protocol == ProtocolType::Resp
                   ? resp_error("ERROR raft_no_quorum")
                   : text_response("ERROR raft_no_quorum");
    }

    {
        std::lock_guard<std::mutex> lock(m_raft_log_mutex);
        if (entry.index > m_raft_commit_index)
        {
            m_raft_commit_index = entry.index;
            apply_committed_raft_entries_locked();
        }
        commit_snapshot = m_raft_commit_index;
        snapshot_index = raft_last_log_index_locked();
        snapshot_term = raft_last_log_term_locked();
    }

    for (const auto &follower : m_router.raft_followers())
    {
        m_router.forward(follower, encode_raft_commit(entry.term, commit_snapshot, snapshot_index, snapshot_term));
    }

    if (request.protocol == ProtocolType::Resp)
    {
        if (request.type == CommandType::Put)
        {
            return resp_simple("OK");
        }
        return resp_integer(1);
    }
    return text_response(request.type == CommandType::Put ? "OK" : "OK");
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

    if (request.type == CommandType::RaftRequestVote)
    {
        std::lock_guard<std::mutex> lock(m_raft_log_mutex);
        return m_router.handle_request_vote(request.term,
                                            request.node_id,
                                            request.last_log_index,
                                            request.last_log_term);
    }

    if (request.type == CommandType::RaftAppendEntries)
    {
        if (request.log_index >= 0)
        {
            return handle_raft_append_entries(request);
        }
        return m_router.handle_append_entries(request.term, request.node_id);
    }

    if (request.type == CommandType::RaftInstallSnapshot)
    {
        std::string heartbeat = m_router.handle_append_entries(request.term, request.node_id);
        if (heartbeat.find(" 0") != std::string::npos)
        {
            return "RAFT_SNAPSHOT " + std::to_string(m_router.current_term()) + " 0\n";
        }
        {
            std::lock_guard<std::mutex> lock(m_raft_log_mutex);
            if (request.last_log_index <= m_raft_last_included_index)
            {
                return "RAFT_SNAPSHOT " + std::to_string(m_router.current_term()) + " 1\n";
            }
        }
        if (!install_snapshot_payload(request.snapshot_payload))
        {
            return "RAFT_SNAPSHOT " + std::to_string(m_router.current_term()) + " 0\n";
        }
        {
            std::lock_guard<std::mutex> lock(m_raft_log_mutex);
            m_raft_last_included_index = request.last_log_index;
            m_raft_last_included_term = request.last_log_term;
            auto keep_begin = std::remove_if(m_raft_log.begin(), m_raft_log.end(), [&](const RaftLogEntry &entry) {
                return entry.index <= m_raft_last_included_index;
            });
            m_raft_log.erase(keep_begin, m_raft_log.end());
            m_raft_commit_index = std::max(m_raft_commit_index, request.last_log_index);
            m_raft_last_applied = std::max(m_raft_last_applied, request.last_log_index);
            persist_raft_locked();
            m_router.update_log_info(raft_last_log_index_locked(), raft_last_log_term_locked());
        }
        return "RAFT_SNAPSHOT " + std::to_string(m_router.current_term()) + " 1\n";
    }

    if (request.type == CommandType::RaftStatus)
    {
        std::lock_guard<std::mutex> lock(m_raft_log_mutex);
        return text_response("RAFT_STATUS node=" + m_router.local_id() +
                             " role=" + m_router.role_name() +
                             " term=" + std::to_string(m_router.current_term()) +
                             " leader=" + m_router.leader_id() +
                             " commit=" + std::to_string(m_raft_commit_index) +
                             " applied=" + std::to_string(m_raft_last_applied) +
                             " last_log=" + std::to_string(raft_last_log_index_locked()));
    }

    if (request.type == CommandType::Metrics)
    {
        return metrics_response();
    }

    if (m_router.raft_enabled() && m_router.should_route(request) && !m_router.is_leader())
    {
        auto leader = m_router.leader_node();
        if (!leader.has_value())
        {
            return request.protocol == ProtocolType::Resp
                       ? resp_error("ERROR leader_unavailable")
                       : text_response("ERROR leader_unavailable");
        }
        std::string response = m_router.forward(*leader, line);
        if (request.protocol == ProtocolType::Resp && response.rfind("ERROR ", 0) == 0)
        {
            return resp_error(strip_line_end(response));
        }
        return response;
    }

    if (m_router.raft_enabled() && is_client_write(request.type) && m_router.is_leader())
    {
        return replicate_raft_write(request);
    }

    if (!m_router.raft_enabled() && m_router.should_route(request) && !m_router.is_local_owner(request.key))
    {
        auto owner = m_router.owner_for(request.key);
        if (!owner.has_value())
        {
            return request.protocol == ProtocolType::Resp
                       ? resp_error("ERROR route_unavailable")
                       : text_response("ERROR route_unavailable");
        }
        std::string response = m_router.forward(*owner, line);
        if (request.protocol == ProtocolType::Resp && response.rfind("ERROR ", 0) == 0)
        {
            return resp_error(strip_line_end(response));
        }
        return response;
    }

    switch (request.type)
    {
    case CommandType::Ping:
        return request.protocol == ProtocolType::Resp ? resp_simple("PONG") : text_response("PONG");
    case CommandType::Put:
    case CommandType::ReplicaPut:
    case CommandType::RaftPut:
    {
        bool ok = request.ttl_seconds > 0
                      ? m_store->put_ttl(request.key, request.value, request.ttl_seconds)
                      : m_store->put(request.key, request.value);
        if (ok && request.type == CommandType::Put && m_router.enabled() && !m_router.raft_enabled())
        {
            for (const auto &replica : m_router.replicas_for(request.key))
            {
                m_router.forward(replica, "REPL_PUT " + request.key + " " + request.value + "\n");
                if (request.ttl_seconds > 0)
                {
                    m_router.forward(replica, "REPL_EXPIRE " + request.key + " " +
                                                  std::to_string(request.ttl_seconds) + "\n");
                }
            }
        }
        if (request.protocol == ProtocolType::Resp)
        {
            return ok ? resp_simple("OK") : resp_error("ERR put_failed");
        }
        return text_response(ok ? "OK" : "ERROR put_failed");
    }
    case CommandType::Get:
    case CommandType::ReplicaGet:
    {
        auto value = m_store->get(request.key);
        if (!value.has_value() && request.type == CommandType::Get && m_router.enabled())
        {
            for (const auto &node : m_router.alive_nodes())
            {
                auto owner = m_router.owner_for(request.key);
                if (owner.has_value() && node.id == owner->id)
                {
                    continue;
                }
                std::string replica_response = m_router.forward(node, "REPL_GET " + request.key + "\n");
                value = parse_text_value(replica_response);
                if (value.has_value())
                {
                    break;
                }
            }
        }
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
    case CommandType::ReplicaDel:
    case CommandType::RaftDel:
    {
        bool removed = m_store->del(request.key);
        if (request.type == CommandType::Del && m_router.enabled() && !m_router.raft_enabled())
        {
            for (const auto &replica : m_router.replicas_for(request.key))
            {
                m_router.forward(replica, "REPL_DEL " + request.key + "\n");
            }
        }
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
    case CommandType::Expire:
    case CommandType::ReplicaExpire:
    case CommandType::RaftExpire:
    {
        bool updated = m_store->expire(request.key, request.ttl_seconds);
        if (updated && request.type == CommandType::Expire && m_router.enabled() && !m_router.raft_enabled())
        {
            for (const auto &replica : m_router.replicas_for(request.key))
            {
                m_router.forward(replica, "REPL_EXPIRE " + request.key + " " +
                                              std::to_string(request.ttl_seconds) + "\n");
            }
        }
        return request.protocol == ProtocolType::Resp
                   ? resp_integer(updated ? 1 : 0)
                   : text_response(updated ? "OK" : "NOT_FOUND");
    }
    case CommandType::Ttl:
    {
        long long ttl = m_store->ttl(request.key);
        return request.protocol == ProtocolType::Resp
                   ? resp_integer(ttl)
                   : text_response("TTL " + std::to_string(ttl));
    }
    case CommandType::RaftRequestVote:
    case CommandType::RaftAppendEntries:
    case CommandType::RaftStatus:
    case CommandType::RaftInstallSnapshot:
    case CommandType::Metrics:
        break;
    case CommandType::Unknown:
        break;
    }

    return "";
}
