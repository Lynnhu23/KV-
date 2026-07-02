#ifndef PROTOCOL_KV_PROTOCOL_H
#define PROTOCOL_KV_PROTOCOL_H

#include "request.h"
#include "response.h"

#include <string>

class KVProtocol
{
public:
    static Request parse_request(const std::string &line);
    static std::string encode_response(const Response &response);
};

#endif
