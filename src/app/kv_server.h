#ifndef APP_KV_SERVER_H
#define APP_KV_SERVER_H

#include "cluster/router.h"
#include "config/config_loader.h"
#include "net/thread_pool.h"
#include "storage/kv_store.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

class KVServer
{
public:
    KVServer();
    ~KVServer();

    bool init(int argc, char *argv[]);
    int start();
    void stop();

private:
    bool init_config(int argc, char *argv[]);
    bool init_log();
    bool init_store();
    bool init_cluster();
    bool init_listener();
    bool init_thread_pool();
    bool init_epoll();
    void event_loop();
    void accept_clients();
    void handle_client_read(int client_fd);
    void close_client(int client_fd);
    void remove_client_from_epoll(int client_fd);
    void rearm_client(int client_fd);
    void process_client_lines(int client_fd, std::vector<std::string> lines, bool close_after_send);
    std::string process_request_line(const std::string &line);
    struct RaftLogEntry
    {
        int index = 0;
        int term = 0;
        CommandType type = CommandType::Unknown;
        std::string key;
        std::string value;
        int ttl_seconds = 0;
    };
    std::string replicate_raft_write(const Request &request);
    std::string handle_raft_append_entries(const Request &request);
    bool apply_raft_entry_locked(const RaftLogEntry &entry);
    void apply_committed_raft_entries_locked();
    int raft_last_log_index_locked() const;
    int raft_last_log_term_locked() const;
    int raft_term_at_locked(int index) const;
    std::string encode_raft_append(const struct RaftLogEntry &entry,
                                   int prev_index,
                                   int prev_term,
                                   int leader_commit) const;
    std::string encode_raft_append_batch(const std::vector<RaftLogEntry> &entries,
                                         int prev_index,
                                         int prev_term,
                                         int leader_commit) const;
    std::string encode_raft_commit(int term, int leader_commit) const;
    bool init_raft_persistence();
    bool load_raft_state();
    bool persist_raft_state_locked() const;
    bool persist_raft_log_locked() const;
    bool persist_raft_locked() const;
    std::vector<RaftLogEntry> decode_raft_entries_payload(const std::string &payload) const;
    std::string encode_snapshot_payload() const;
    bool install_snapshot_payload(const std::string &payload);
    void record_request_metrics(long long latency_us, bool failed);
    std::string metrics_response();

private:
    ConfigLoader m_config;
    ClusterRouter m_router;
    std::unique_ptr<KVStore> m_store;
    std::unique_ptr<ThreadPool> m_thread_pool;
    int m_listenfd;
    int m_epollfd;
    std::atomic<bool> m_running;
    std::mutex m_clients_mutex;
    std::unordered_map<int, std::string> m_client_buffers;
    std::mutex m_raft_log_mutex;
    std::vector<RaftLogEntry> m_raft_log;
    int m_raft_commit_index = 0;
    int m_raft_last_applied = 0;
    std::string m_raft_log_file;
    std::string m_raft_state_file;
    std::chrono::steady_clock::time_point m_metrics_started_at;
    std::atomic<unsigned long long> m_requests_total{0};
    std::atomic<unsigned long long> m_failed_requests_total{0};
    std::atomic<unsigned long long> m_replication_failures_total{0};
    std::mutex m_latency_mutex;
    std::vector<long long> m_recent_latencies_us;
};

#endif
