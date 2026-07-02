#ifndef SERVER_CONFIG_H
#define SERVER_CONFIG_H

#include <string>
#include <string_view>
#include <vector>

struct ClusterPeerConfig
{
    std::string id;
    std::string host;
    int port = 0;
};

struct ServerConfig
{
    struct Node
    {
        std::string id = "node-1";
        std::string host = "0.0.0.0";
        int port = 9006;
        std::string data_dir = "./data/node-1";
    } node;

    struct Store
    {
        std::string engine = "memory";
        bool wal_enabled = false;
        std::string wal_file = "./data/node-1/kv.wal";
        std::string snapshot_file = "./data/node-1/kv.snapshot";
        int snapshot_threshold = 1000;
        int max_keys = 0;
    } store;

    struct Cluster
    {
        bool enabled = false;
        std::vector<ClusterPeerConfig> peers;
        int replication_factor = 2;
        int health_check_interval_ms = 1000;
        std::string consistency = "best_effort";
        std::string leader_id = "node-1";
    } cluster;

    struct LogConfig
    {
        std::string write_mode = "async";   // "sync" | "async"
        std::string file = "./KVServerLog";
        int buffer_size = 8192;
        int split_lines = 5000000;
        int max_queue_size = 800;
        bool close = false;
    } log;

    bool validate() const;
};

#endif
