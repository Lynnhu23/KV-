#include "kv_protocol.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <vector>

namespace
{
std::string trim(std::string value)
{
    auto first = std::find_if_not(value.begin(), value.end(), [](unsigned char ch) {
        return std::isspace(ch);
    });
    auto last = std::find_if_not(value.rbegin(), value.rend(), [](unsigned char ch) {
        return std::isspace(ch);
    }).base();

    if (first >= last)
    {
        return "";
    }
    return std::string(first, last);
}

std::string upper(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return value;
}

Request error_request(const std::string &message)
{
    Request request;
    request.error = message;
    return request;
}

Request error_request(const std::string &message, ProtocolType protocol)
{
    Request request;
    request.protocol = protocol;
    request.error = message;
    return request;
}

bool parse_long(const std::string &text, long &out)
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

int hex_value(char ch)
{
    if (ch >= '0' && ch <= '9')
    {
        return ch - '0';
    }
    if (ch >= 'a' && ch <= 'f')
    {
        return ch - 'a' + 10;
    }
    if (ch >= 'A' && ch <= 'F')
    {
        return ch - 'A' + 10;
    }
    return -1;
}

bool decode_hex(const std::string &encoded, std::string &decoded)
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
        int high = hex_value(encoded[i]);
        int low = hex_value(encoded[i + 1]);
        if (high < 0 || low < 0)
        {
            return false;
        }
        decoded.push_back(static_cast<char>((high << 4) | low));
    }
    return true;
}

bool read_crlf_line(const std::string &frame, size_t &pos, std::string &line)
{
    size_t end = frame.find("\r\n", pos);
    if (end == std::string::npos)
    {
        return false;
    }
    line = frame.substr(pos, end - pos);
    pos = end + 2;
    return true;
}

Request parse_args(const std::vector<std::string> &args, ProtocolType protocol)
{
    if (args.empty())
    {
        return error_request("ERROR empty_command", protocol);
    }

    std::string command = upper(args[0]);
    Request request;
    request.protocol = protocol;

    if (command == "PING")
    {
        if (args.size() != 1)
        {
            return error_request("ERROR usage: PING", protocol);
        }
        request.type = CommandType::Ping;
        return request;
    }

    if (command == "PUT" || command == "SET" || command == "REPL_PUT" || command == "RAFT_PUT")
    {
        if (!((args.size() == 3) || (args.size() == 5 && upper(args[3]) == "EX")) ||
            args[1].empty() || args[2].empty())
        {
            return error_request("ERROR usage: SET <key> <value> [EX seconds]", protocol);
        }
        request.type = CommandType::Put;
        if (command == "REPL_PUT")
        {
            request.type = CommandType::ReplicaPut;
        }
        if (command == "RAFT_PUT")
        {
            request.type = CommandType::RaftPut;
        }
        request.key = args[1];
        request.value = args[2];
        if (args.size() == 5)
        {
            long ttl = 0;
            if (!parse_long(args[4], ttl) || ttl <= 0)
            {
                return error_request("ERROR invalid_ttl", protocol);
            }
            request.ttl_seconds = static_cast<int>(ttl);
        }
        return request;
    }

    if (command == "GET")
    {
        if (args.size() != 2 || args[1].empty())
        {
            return error_request("ERROR usage: GET <key>", protocol);
        }
        request.type = CommandType::Get;
        request.key = args[1];
        return request;
    }

    if (command == "REPL_GET")
    {
        if (args.size() != 2 || args[1].empty())
        {
            return error_request("ERROR usage: REPL_GET <key>", protocol);
        }
        request.type = CommandType::ReplicaGet;
        request.key = args[1];
        return request;
    }

    if (command == "DEL" || command == "REPL_DEL" || command == "RAFT_DEL")
    {
        if (args.size() != 2 || args[1].empty())
        {
            return error_request("ERROR usage: DEL <key>", protocol);
        }
        request.type = CommandType::Del;
        if (command == "REPL_DEL")
        {
            request.type = CommandType::ReplicaDel;
        }
        if (command == "RAFT_DEL")
        {
            request.type = CommandType::RaftDel;
        }
        request.key = args[1];
        return request;
    }

    if (command == "EXISTS")
    {
        if (args.size() != 2 || args[1].empty())
        {
            return error_request("ERROR usage: EXISTS <key>", protocol);
        }
        request.type = CommandType::Exists;
        request.key = args[1];
        return request;
    }

    if (command == "EXPIRE" || command == "REPL_EXPIRE" || command == "RAFT_EXPIRE")
    {
        if (args.size() != 3 || args[1].empty())
        {
            return error_request("ERROR usage: EXPIRE <key> <seconds>", protocol);
        }
        long ttl = 0;
        if (!parse_long(args[2], ttl) || ttl <= 0)
        {
            return error_request("ERROR invalid_ttl", protocol);
        }
        request.type = CommandType::Expire;
        if (command == "REPL_EXPIRE")
        {
            request.type = CommandType::ReplicaExpire;
        }
        if (command == "RAFT_EXPIRE")
        {
            request.type = CommandType::RaftExpire;
        }
        request.key = args[1];
        request.ttl_seconds = static_cast<int>(ttl);
        return request;
    }

    if (command == "TTL")
    {
        if (args.size() != 2 || args[1].empty())
        {
            return error_request("ERROR usage: TTL <key>", protocol);
        }
        request.type = CommandType::Ttl;
        request.key = args[1];
        return request;
    }

    if (command == "RAFT_REQUEST_VOTE")
    {
        if (args.size() != 3 && args.size() != 5)
        {
            return error_request("ERROR usage: RAFT_REQUEST_VOTE <term> <candidate_id> [last_log_index last_log_term]", protocol);
        }
        long term = 0;
        if (!parse_long(args[1], term) || term < 0)
        {
            return error_request("ERROR invalid_term", protocol);
        }
        request.type = CommandType::RaftRequestVote;
        request.term = static_cast<int>(term);
        request.node_id = args[2];
        if (args.size() == 5)
        {
            long last_index = 0;
            long last_term = 0;
            if (!parse_long(args[3], last_index) || last_index < 0 ||
                !parse_long(args[4], last_term) || last_term < 0)
            {
                return error_request("ERROR invalid_raft_vote_log", protocol);
            }
            request.last_log_index = static_cast<int>(last_index);
            request.last_log_term = static_cast<int>(last_term);
        }
        return request;
    }

    if (command == "RAFT_APPEND_ENTRIES")
    {
        if (args.size() != 3 && args.size() != 8 && args.size() != 12)
        {
            return error_request("ERROR usage: RAFT_APPEND_ENTRIES <term> <leader_id> [prev_index prev_term leader_commit entry_count payload_hex]", protocol);
        }
        long term = 0;
        if (!parse_long(args[1], term) || term < 0)
        {
            return error_request("ERROR invalid_term", protocol);
        }
        request.type = CommandType::RaftAppendEntries;
        request.term = static_cast<int>(term);
        request.node_id = args[2];
        if (args.size() == 3)
        {
            return request;
        }

        if (args.size() == 8)
        {
            long prev_index = 0;
            long prev_term = 0;
            long leader_commit = 0;
            long entry_count = 0;
            if (!parse_long(args[3], prev_index) || prev_index < 0 ||
                !parse_long(args[4], prev_term) || prev_term < 0 ||
                !parse_long(args[5], leader_commit) || leader_commit < 0 ||
                !parse_long(args[6], entry_count) || entry_count < 0)
            {
                return error_request("ERROR invalid_raft_append", protocol);
            }
            std::string payload;
            if (!decode_hex(args[7], payload))
            {
                return error_request("ERROR invalid_raft_hex", protocol);
            }
            request.prev_log_index = static_cast<int>(prev_index);
            request.prev_log_term = static_cast<int>(prev_term);
            request.leader_commit = static_cast<int>(leader_commit);
            request.entry_count = static_cast<int>(entry_count);
            request.snapshot_payload = payload;
            request.log_index = 0;
            return request;
        }

        long prev_index = 0;
        long prev_term = 0;
        long leader_commit = 0;
        long entry_index = 0;
        long entry_term = 0;
        long ttl = 0;
        if (!parse_long(args[3], prev_index) || prev_index < 0 ||
            !parse_long(args[4], prev_term) || prev_term < 0 ||
            !parse_long(args[5], leader_commit) || leader_commit < 0 ||
            !parse_long(args[6], entry_index) || entry_index < 0 ||
            !parse_long(args[7], entry_term) || entry_term < 0 ||
            !parse_long(args[10], ttl) || ttl < 0)
        {
            return error_request("ERROR invalid_raft_append", protocol);
        }

        std::string key;
        std::string value;
        if (!decode_hex(args[9], key) || !decode_hex(args[11], value))
        {
            return error_request("ERROR invalid_raft_hex", protocol);
        }

        request.prev_log_index = static_cast<int>(prev_index);
        request.prev_log_term = static_cast<int>(prev_term);
        request.leader_commit = static_cast<int>(leader_commit);
        request.log_index = static_cast<int>(entry_index);
        request.log_term = static_cast<int>(entry_term);
        request.raft_op = upper(args[8]);
        request.key = key;
        request.value = value;
        request.ttl_seconds = static_cast<int>(ttl);
        return request;
    }

    if (command == "RAFT_INSTALL_SNAPSHOT")
    {
        if (args.size() != 6)
        {
            return error_request("ERROR usage: RAFT_INSTALL_SNAPSHOT <term> <leader_id> <last_index> <last_term> <payload_hex>", protocol);
        }
        long term = 0;
        long last_index = 0;
        long last_term = 0;
        if (!parse_long(args[1], term) || term < 0 ||
            !parse_long(args[3], last_index) || last_index < 0 ||
            !parse_long(args[4], last_term) || last_term < 0)
        {
            return error_request("ERROR invalid_snapshot_header", protocol);
        }
        std::string payload;
        if (!decode_hex(args[5], payload))
        {
            return error_request("ERROR invalid_raft_hex", protocol);
        }
        request.type = CommandType::RaftInstallSnapshot;
        request.term = static_cast<int>(term);
        request.node_id = args[2];
        request.last_log_index = static_cast<int>(last_index);
        request.last_log_term = static_cast<int>(last_term);
        request.snapshot_payload = payload;
        return request;
    }

    if (command == "RAFT_STATUS")
    {
        if (args.size() != 1)
        {
            return error_request("ERROR usage: RAFT_STATUS", protocol);
        }
        request.type = CommandType::RaftStatus;
        return request;
    }

    if (command == "METRICS")
    {
        if (args.size() != 1)
        {
            return error_request("ERROR usage: METRICS", protocol);
        }
        request.type = CommandType::Metrics;
        return request;
    }

    return error_request("ERROR unknown_command", protocol);
}

Request parse_resp_request(const std::string &frame)
{
    size_t pos = 0;
    std::string line;
    if (!read_crlf_line(frame, pos, line) || line.empty() || line[0] != '*')
    {
        return error_request("ERROR invalid_resp", ProtocolType::Resp);
    }

    long count = 0;
    if (!parse_long(line.substr(1), count) || count <= 0)
    {
        return error_request("ERROR invalid_resp", ProtocolType::Resp);
    }

    std::vector<std::string> args;
    args.reserve(static_cast<size_t>(count));
    for (long i = 0; i < count; ++i)
    {
        if (!read_crlf_line(frame, pos, line) || line.empty() || line[0] != '$')
        {
            return error_request("ERROR invalid_resp", ProtocolType::Resp);
        }

        long bulk_len = 0;
        if (!parse_long(line.substr(1), bulk_len) || bulk_len < 0)
        {
            return error_request("ERROR invalid_resp", ProtocolType::Resp);
        }

        size_t len = static_cast<size_t>(bulk_len);
        if (pos + len + 2 > frame.size() || frame.compare(pos + len, 2, "\r\n") != 0)
        {
            return error_request("ERROR invalid_resp", ProtocolType::Resp);
        }

        args.push_back(frame.substr(pos, len));
        pos += len + 2;
    }

    if (pos != frame.size())
    {
        return error_request("ERROR invalid_resp", ProtocolType::Resp);
    }

    return parse_args(args, ProtocolType::Resp);
}
}

Request KVProtocol::parse_request(const std::string &line)
{
    if (!line.empty() && line.front() == '*')
    {
        return parse_resp_request(line);
    }

    std::string normalized = trim(line);
    if (normalized.empty())
    {
        return error_request("");
    }

    std::istringstream input(normalized);
    std::string command;
    input >> command;
    command = upper(command);

    if (command == "PUT" || command == "REPL_PUT" || command == "RAFT_PUT")
    {
        std::string key;
        std::string value;
        input >> key;
        std::getline(input, value);
        value = trim(value);

        if (key.empty() || value.empty())
        {
            return error_request("ERROR usage: PUT <key> <value>");
        }

        return parse_args({command, key, value}, ProtocolType::Text);
    }

    std::vector<std::string> args;
    args.push_back(command);
    std::string token;
    while (input >> token)
    {
        args.push_back(token);
    }
    return parse_args(args, ProtocolType::Text);
}

std::string KVProtocol::encode_response(const Response &response)
{
    if (response.text.empty())
    {
        return "";
    }
    return response.text + "\n";
}
