#include "config/yaml_parser.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <vector>
#include "log/log.h"

// ---- 轻量级 YAML 子集解析器 ----
// 支持: 注释(#), 缩进嵌套, key: value, 引号字符串, 整数, 布尔值
// 不依赖外部库 (yaml-cpp 可选)

namespace
{
    // 去除首尾空白
    std::string trim(std::string s)
    {
        auto start = s.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) return "";
        auto end = s.find_last_not_of(" \t\r\n");
        return s.substr(start, end - start + 1);
    }

    // 去除注释
    std::string strip_comment(std::string s)
    {
        auto pos = s.find('#');
        if (pos != std::string::npos)
            s = s.substr(0, pos);
        return trim(s);
    }

    // 获取缩进级别
    int indent_level(const std::string &line)
    {
        int n = 0;
        for (char c : line)
        {
            if (c == ' ') ++n;
            else if (c == '\t') n += 2;  // tab = 2 spaces
            else break;
        }
        return n;
    }

    // 去除引号
    std::string unquote(std::string s)
    {
        if (s.size() >= 2)
        {
            if ((s.front() == '"' && s.back() == '"') ||
                (s.front() == '\'' && s.back() == '\''))
                return s.substr(1, s.size() - 2);
        }
        return s;
    }

    // 解析布尔值
    bool parse_bool(const std::string &s, bool &out)
    {
        std::string lower = s;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        if (lower == "true" || lower == "yes" || lower == "on") { out = true; return true; }
        if (lower == "false" || lower == "no" || lower == "off") { out = false; return true; }
        return false;
    }

    std::vector<std::string> split(const std::string &s, char delimiter)
    {
        std::vector<std::string> parts;
        std::string current;
        std::istringstream input(s);
        while (std::getline(input, current, delimiter))
        {
            current = trim(current);
            if (!current.empty())
            {
                parts.push_back(current);
            }
        }
        return parts;
    }

    bool parse_peer(const std::string &text, ClusterPeerConfig &peer)
    {
        auto at_pos = text.find('@');
        auto colon_pos = text.rfind(':');
        if (at_pos == std::string::npos || colon_pos == std::string::npos || at_pos > colon_pos)
        {
            return false;
        }

        peer.id = trim(text.substr(0, at_pos));
        peer.host = trim(text.substr(at_pos + 1, colon_pos - at_pos - 1));
        peer.port = std::stoi(trim(text.substr(colon_pos + 1)));
        return !peer.id.empty() && !peer.host.empty();
    }

    std::vector<ClusterPeerConfig> parse_peers(const std::string &value)
    {
        std::vector<ClusterPeerConfig> peers;
        for (const auto &part : split(value, ','))
        {
            ClusterPeerConfig peer;
            if (parse_peer(part, peer))
            {
                peers.push_back(peer);
            }
        }
        return peers;
    }
}

std::optional<ServerConfig> YamlConfigParser::load(const std::string &path)
{
    std::ifstream file(path);
    if (!file.is_open())
    {
        LOG_ERROR("Cannot open config file: %s", path.c_str());
        return std::nullopt;
    }

    ServerConfig cfg;
    std::string line;
    std::string current_section;

    while (std::getline(file, line))
    {
        std::string stripped = strip_comment(line);
        if (stripped.empty()) continue;

        int indent = indent_level(line);

        // 顶层键 (0 缩进) → section
        if (indent == 0 && stripped.back() == ':')
        {
            current_section = trim(stripped.substr(0, stripped.size() - 1));
            continue;
        }

        // 解析 key: value
        auto colon_pos = stripped.find(':');
        if (colon_pos == std::string::npos) continue;

        std::string key = trim(stripped.substr(0, colon_pos));
        std::string value = trim(stripped.substr(colon_pos + 1));
        value = unquote(value);

        if (key.empty() || value.empty()) continue;

        // 路由到正确的 section
        try
        {
            if (current_section == "node")
            {
                if (key == "id") cfg.node.id = value;
                else if (key == "host") cfg.node.host = value;
                else if (key == "port") cfg.node.port = std::stoi(value);
                else if (key == "data_dir") cfg.node.data_dir = value;
            }
            else if (current_section == "store")
            {
                if (key == "engine") cfg.store.engine = value;
                else if (key == "wal_enabled") { bool b; if (parse_bool(value, b)) cfg.store.wal_enabled = b; }
                else if (key == "wal_file") cfg.store.wal_file = value;
                else if (key == "snapshot_file") cfg.store.snapshot_file = value;
                else if (key == "snapshot_threshold") cfg.store.snapshot_threshold = std::stoi(value);
                else if (key == "max_keys") cfg.store.max_keys = std::stoi(value);
            }
            else if (current_section == "cluster")
            {
                if (key == "enabled") { bool b; if (parse_bool(value, b)) cfg.cluster.enabled = b; }
                else if (key == "peers") cfg.cluster.peers = parse_peers(value);
                else if (key == "replication_factor") cfg.cluster.replication_factor = std::stoi(value);
                else if (key == "health_check_interval_ms") cfg.cluster.health_check_interval_ms = std::stoi(value);
                else if (key == "consistency") cfg.cluster.consistency = value;
                else if (key == "leader_id") cfg.cluster.leader_id = value;
            }
            else if (current_section == "log")
            {
                if (key == "write_mode") cfg.log.write_mode = value;
                else if (key == "file") cfg.log.file = value;
                else if (key == "buffer_size") cfg.log.buffer_size = std::stoi(value);
                else if (key == "split_lines") cfg.log.split_lines = std::stoi(value);
                else if (key == "max_queue_size") cfg.log.max_queue_size = std::stoi(value);
                else if (key == "close") { bool b; if (parse_bool(value, b)) cfg.log.close = b; }
            }
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("Config parse error [%s].%s: %s", current_section.c_str(), key.c_str(), e.what());
        }
    }

    if (!cfg.validate())
    {
        LOG_ERROR("Config validation failed");
        return std::nullopt;
    }

    return cfg;
}

std::string YamlConfigParser::default_config_yaml()
{
    return R"(# TinyKVServer Configuration
node:
  id: "node-1"
  host: "0.0.0.0"
  port: 9006
  data_dir: "./data/node-1"

store:
  engine: "memory"
  wal_enabled: false
  wal_file: "./data/node-1/kv.wal"
  snapshot_file: "./data/node-1/kv.snapshot"
  snapshot_threshold: 1000
  max_keys: 0

cluster:
  enabled: false
  peers: "node-1@127.0.0.1:9006"
  replication_factor: 2
  health_check_interval_ms: 1000
  consistency: "best_effort"
  leader_id: "node-1"

log:
  write_mode: "async"
  file: "./KVServerLog"
  buffer_size: 8192
  split_lines: 5000000
  max_queue_size: 800
  close: false
)";
}

std::string YamlConfigParser::to_yaml(const ServerConfig &cfg)
{
    // 简单序列化为 YAML 格式
    std::ostringstream oss;
    oss << "node:\n"
        << "  id: \"" << cfg.node.id << "\"\n"
        << "  host: \"" << cfg.node.host << "\"\n"
        << "  port: " << cfg.node.port << "\n"
        << "  data_dir: \"" << cfg.node.data_dir << "\"\n\n"
        << "store:\n"
        << "  engine: \"" << cfg.store.engine << "\"\n"
        << "  wal_enabled: " << (cfg.store.wal_enabled ? "true" : "false") << "\n"
        << "  wal_file: \"" << cfg.store.wal_file << "\"\n"
        << "  snapshot_file: \"" << cfg.store.snapshot_file << "\"\n"
        << "  snapshot_threshold: " << cfg.store.snapshot_threshold << "\n"
        << "  max_keys: " << cfg.store.max_keys << "\n\n"
        << "cluster:\n"
        << "  enabled: " << (cfg.cluster.enabled ? "true" : "false") << "\n"
        << "  peers: \"";
    for (size_t i = 0; i < cfg.cluster.peers.size(); ++i)
    {
        if (i > 0) oss << ",";
        const auto &peer = cfg.cluster.peers[i];
        oss << peer.id << "@" << peer.host << ":" << peer.port;
    }
    oss << "\"\n"
        << "  replication_factor: " << cfg.cluster.replication_factor << "\n"
        << "  health_check_interval_ms: " << cfg.cluster.health_check_interval_ms << "\n"
        << "  consistency: \"" << cfg.cluster.consistency << "\"\n"
        << "  leader_id: \"" << cfg.cluster.leader_id << "\"\n\n"
        << "log:\n"
        << "  write_mode: \"" << cfg.log.write_mode << "\"\n"
        << "  file: \"" << cfg.log.file << "\"\n"
        << "  buffer_size: " << cfg.log.buffer_size << "\n"
        << "  split_lines: " << cfg.log.split_lines << "\n"
        << "  max_queue_size: " << cfg.log.max_queue_size << "\n"
        << "  close: " << (cfg.log.close ? "true" : "false") << "\n";
    return oss.str();
}

// ---- ServerConfig 验证 ----
bool ServerConfig::validate() const
{
    if (node.id.empty())
    {
        LOG_ERROR("Invalid node id");
        return false;
    }
    if (node.host.empty())
    {
        LOG_ERROR("Invalid node host");
        return false;
    }
    if (node.port < 1 || node.port > 65535)
    {
        LOG_ERROR("Invalid node port: %d", node.port);
        return false;
    }
    if (store.engine != "memory")
    {
        LOG_ERROR("Unsupported store engine: %s", store.engine.c_str());
        return false;
    }
    if (store.snapshot_threshold < 0)
    {
        LOG_ERROR("Invalid snapshot threshold: %d", store.snapshot_threshold);
        return false;
    }
    if (store.max_keys < 0)
    {
        LOG_ERROR("Invalid max keys: %d", store.max_keys);
        return false;
    }
    if (cluster.enabled && cluster.peers.empty())
    {
        LOG_ERROR("Cluster enabled but peers is empty");
        return false;
    }
    if (cluster.replication_factor < 1)
    {
        LOG_ERROR("Invalid replication factor: %d", cluster.replication_factor);
        return false;
    }
    if (cluster.health_check_interval_ms < 100)
    {
        LOG_ERROR("Invalid health check interval: %d", cluster.health_check_interval_ms);
        return false;
    }
    if (cluster.consistency != "best_effort" && cluster.consistency != "raft")
    {
        LOG_ERROR("Invalid consistency mode: %s", cluster.consistency.c_str());
        return false;
    }
    for (const auto &peer : cluster.peers)
    {
        if (peer.id.empty() || peer.host.empty() || peer.port < 1 || peer.port > 65535)
        {
            LOG_ERROR("Invalid cluster peer");
            return false;
        }
    }
    return true;
}
