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

    if (command == "PUT" || command == "SET")
    {
        if (args.size() != 3 || args[1].empty() || args[2].empty())
        {
            return error_request("ERROR usage: SET <key> <value>", protocol);
        }
        request.type = CommandType::Put;
        request.key = args[1];
        request.value = args[2];
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

    if (command == "DEL")
    {
        if (args.size() != 2 || args[1].empty())
        {
            return error_request("ERROR usage: DEL <key>", protocol);
        }
        request.type = CommandType::Del;
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

    if (command == "PUT" || command == "SET")
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
