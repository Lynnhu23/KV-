#ifndef PROTOCOL_REQUEST_H
#define PROTOCOL_REQUEST_H

#include <string>

enum class CommandType
{
    Ping,
    Put,
    Get,
    Del,
    Exists,
    Expire,
    Ttl,
    ReplicaPut,
    ReplicaGet,
    ReplicaDel,
    ReplicaExpire,
    RaftPut,
    RaftDel,
    RaftExpire,
    Unknown
};

enum class ProtocolType
{
    Text,
    Resp
};

struct Request
{
    CommandType type = CommandType::Unknown;
    ProtocolType protocol = ProtocolType::Text;
    std::string key;
    std::string value;
    int ttl_seconds = 0;
    std::string error;
};

#endif
