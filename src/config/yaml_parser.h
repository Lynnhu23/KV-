#ifndef YAML_PARSER_H
#define YAML_PARSER_H

#include "server_config.h"
#include <string>
#include <optional>

class YamlConfigParser
{
public:
    // 从 YAML 文件加载配置，失败返回 nullopt
    static std::optional<ServerConfig> load(const std::string &path);

    // 生成默认 YAML 内容字符串 (首次运行时写入)
    static std::string default_config_yaml();

    // 将 ServerConfig 序列化回 YAML 字符串
    static std::string to_yaml(const ServerConfig &cfg);
};

#endif
