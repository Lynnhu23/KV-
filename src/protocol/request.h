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
    RaftRequestVote,
    RaftAppendEntries,
    RaftStatus,
    RaftInstallSnapshot,
    Metrics,
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
    int term = 0;
    std::string node_id;
    int prev_log_index = 0;
    int prev_log_term = 0;
    int leader_commit = 0;
    int log_index = -1;
    int log_term = 0;
    std::string raft_op;
    int last_log_index = 0;
    int last_log_term = 0;
    int entry_count = 0;
    std::string snapshot_payload;
    std::string error;
};

#endif
