#ifndef CONFIG_CONFIG_LOADER_H
#define CONFIG_CONFIG_LOADER_H

#include "server_config.h"
#include <string>
#include <string_view>

using namespace std;

class ConfigLoader
{
public:
    ConfigLoader();
    ~ConfigLoader() = default;

    void parse_arg(int argc, char *argv[]);

    // 最终配置 (YAML + CLI 叠加)
    ServerConfig server_config;

private:
    std::string m_config_path;
};

#endif
