#include "config/config_loader.h"
#include "config/yaml_parser.h"
#include "log/log.h"

#include <string>
#include <string_view>
#include <fstream>
#include <unistd.h>

ConfigLoader::ConfigLoader()
{
    m_config_path = "./configs/kvserver.yaml";
}

void ConfigLoader::parse_arg(int argc, char *argv[])
{
    // 1. 先解析 CLI 参数 (主要是 -f 指定配置文件路径)
    int opt;
    constexpr std::string_view optstring = "p:l:c:f:";
    bool has_port = false;
    bool has_log_write = false;
    bool has_close_log = false;
    int port = 0;
    int log_write = 0;
    int close_log = 0;

    while ((opt = getopt(argc, argv, optstring.data())) != -1)
    {
        switch (opt)
        {
        case 'f':
            m_config_path = std::string(optarg);
            break;
        case 'p': port = std::stoi(optarg); has_port = true; break;
        case 'l': log_write = std::stoi(optarg); has_log_write = true; break;
        case 'c': close_log = std::stoi(optarg); has_close_log = true; break;
        default: break;
        }
    }

    // 2. 尝试加载 YAML 配置文件
    auto yaml_cfg = YamlConfigParser::load(m_config_path);
    if (yaml_cfg.has_value())
    {
        server_config = std::move(*yaml_cfg);
    }
    else
    {
        // 配置文件不存在: 生成默认配置
        LOG_INFO("ConfigLoader file not found, creating default: %s", m_config_path.c_str());
        std::ofstream out(m_config_path);
        if (out.is_open())
        {
            out << YamlConfigParser::default_config_yaml();
            out.close();
        }
        server_config = ServerConfig{};  // 使用默认值
    }

    // 3. CLI 参数覆盖 YAML 配置
    if (has_port)
    {
        server_config.node.port = port;
    }
    if (has_log_write)
    {
        server_config.log.write_mode = (log_write == 1) ? "async" : "sync";
    }
    if (has_close_log)
    {
        server_config.log.close = (close_log == 1);
    }
}
