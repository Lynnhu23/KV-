#ifndef APP_KV_SERVER_H
#define APP_KV_SERVER_H

#include "cluster/router.h"
#include "config/config_loader.h"
#include "net/thread_pool.h"
#include "storage/kv_store.h"

#include <atomic>
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
};

#endif
