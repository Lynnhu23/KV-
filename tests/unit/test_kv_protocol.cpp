#include <gtest/gtest.h>

#include "protocol/kv_protocol.h"

TEST(KVProtocolTest, ParsePutWithSpaces)
{
    Request request = KVProtocol::parse_request("PUT title distributed kv");

    EXPECT_EQ(request.type, CommandType::Put);
    EXPECT_EQ(request.key, "title");
    EXPECT_EQ(request.value, "distributed kv");
    EXPECT_TRUE(request.error.empty());
}

TEST(KVProtocolTest, ParseGetAndExists)
{
    Request get = KVProtocol::parse_request("get user:1");
    Request exists = KVProtocol::parse_request("EXISTS user:1");

    EXPECT_EQ(get.type, CommandType::Get);
    EXPECT_EQ(get.key, "user:1");
    EXPECT_EQ(exists.type, CommandType::Exists);
    EXPECT_EQ(exists.key, "user:1");
}

TEST(KVProtocolTest, RejectBadCommands)
{
    EXPECT_EQ(KVProtocol::parse_request("PUT only_key").error,
              "ERROR usage: PUT <key> <value>");
    EXPECT_EQ(KVProtocol::parse_request("GET a b").error,
              "ERROR usage: GET <key>");
    EXPECT_EQ(KVProtocol::parse_request("UNKNOWN").error,
              "ERROR unknown_command");
}

TEST(KVProtocolTest, EncodeResponse)
{
    EXPECT_EQ(KVProtocol::encode_response({"OK"}), "OK\n");
    EXPECT_EQ(KVProtocol::encode_response({""}), "");
}

TEST(KVProtocolTest, ParseRespSetGetExists)
{
    Request set = KVProtocol::parse_request("*3\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$5\r\nalice\r\n");
    EXPECT_EQ(set.protocol, ProtocolType::Resp);
    EXPECT_EQ(set.type, CommandType::Put);
    EXPECT_EQ(set.key, "user:1");
    EXPECT_EQ(set.value, "alice");
    EXPECT_TRUE(set.error.empty());

    Request get = KVProtocol::parse_request("*2\r\n$3\r\nGET\r\n$6\r\nuser:1\r\n");
    EXPECT_EQ(get.protocol, ProtocolType::Resp);
    EXPECT_EQ(get.type, CommandType::Get);
    EXPECT_EQ(get.key, "user:1");

    Request exists = KVProtocol::parse_request("*2\r\n$6\r\nEXISTS\r\n$6\r\nuser:1\r\n");
    EXPECT_EQ(exists.protocol, ProtocolType::Resp);
    EXPECT_EQ(exists.type, CommandType::Exists);
    EXPECT_EQ(exists.key, "user:1");
}

TEST(KVProtocolTest, ParseRespSetWithExpireAndTtl)
{
    Request set = KVProtocol::parse_request(
        "*5\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$5\r\nalice\r\n$2\r\nEX\r\n$2\r\n60\r\n");
    EXPECT_EQ(set.protocol, ProtocolType::Resp);
    EXPECT_EQ(set.type, CommandType::Put);
    EXPECT_EQ(set.key, "user:1");
    EXPECT_EQ(set.value, "alice");
    EXPECT_EQ(set.ttl_seconds, 60);

    Request ttl = KVProtocol::parse_request("*2\r\n$3\r\nTTL\r\n$6\r\nuser:1\r\n");
    EXPECT_EQ(ttl.protocol, ProtocolType::Resp);
    EXPECT_EQ(ttl.type, CommandType::Ttl);

    Request expire = KVProtocol::parse_request("*3\r\n$6\r\nEXPIRE\r\n$6\r\nuser:1\r\n$2\r\n30\r\n");
    EXPECT_EQ(expire.protocol, ProtocolType::Resp);
    EXPECT_EQ(expire.type, CommandType::Expire);
    EXPECT_EQ(expire.ttl_seconds, 30);
}

TEST(KVProtocolTest, RejectBadResp)
{
    Request request = KVProtocol::parse_request("*2\r\n$3\r\nGET\r\n$6\r\nuser:1");
    EXPECT_EQ(request.protocol, ProtocolType::Resp);
    EXPECT_EQ(request.error, "ERROR invalid_resp");
}
