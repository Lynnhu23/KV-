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

TEST(KVProtocolTest, ParseRaftControlMessages)
{
    Request vote = KVProtocol::parse_request("RAFT_REQUEST_VOTE 7 node-2");
    EXPECT_EQ(vote.type, CommandType::RaftRequestVote);
    EXPECT_EQ(vote.term, 7);
    EXPECT_EQ(vote.node_id, "node-2");
    EXPECT_TRUE(vote.error.empty());

    Request vote_with_log = KVProtocol::parse_request("RAFT_REQUEST_VOTE 9 node-4 12 8");
    EXPECT_EQ(vote_with_log.type, CommandType::RaftRequestVote);
    EXPECT_EQ(vote_with_log.last_log_index, 12);
    EXPECT_EQ(vote_with_log.last_log_term, 8);

    Request append = KVProtocol::parse_request("RAFT_APPEND_ENTRIES 8 node-3");
    EXPECT_EQ(append.type, CommandType::RaftAppendEntries);
    EXPECT_EQ(append.term, 8);
    EXPECT_EQ(append.node_id, "node-3");
    EXPECT_TRUE(append.error.empty());

    Request append_log = KVProtocol::parse_request(
        "RAFT_APPEND_ENTRIES 8 node-3 1 7 1 2 8 PUT 757365723a31 0 616c696365");
    EXPECT_EQ(append_log.type, CommandType::RaftAppendEntries);
    EXPECT_EQ(append_log.term, 8);
    EXPECT_EQ(append_log.node_id, "node-3");
    EXPECT_EQ(append_log.prev_log_index, 1);
    EXPECT_EQ(append_log.prev_log_term, 7);
    EXPECT_EQ(append_log.leader_commit, 1);
    EXPECT_EQ(append_log.log_index, 2);
    EXPECT_EQ(append_log.log_term, 8);
    EXPECT_EQ(append_log.raft_op, "PUT");
    EXPECT_EQ(append_log.key, "user:1");
    EXPECT_EQ(append_log.value, "alice");
    EXPECT_TRUE(append_log.error.empty());

    Request append_batch = KVProtocol::parse_request(
        "RAFT_APPEND_ENTRIES 9 node-1 2 8 2 2 332039205055542061203020620a3420392044454c20632030202d0a");
    EXPECT_EQ(append_batch.type, CommandType::RaftAppendEntries);
    EXPECT_EQ(append_batch.prev_log_index, 2);
    EXPECT_EQ(append_batch.prev_log_term, 8);
    EXPECT_EQ(append_batch.leader_commit, 2);
    EXPECT_EQ(append_batch.entry_count, 2);
    EXPECT_EQ(append_batch.snapshot_payload, "3 9 PUT a 0 b\n4 9 DEL c 0 -\n");

    Request install = KVProtocol::parse_request(
        "RAFT_INSTALL_SNAPSHOT 10 node-1 20 9 61206220300a");
    EXPECT_EQ(install.type, CommandType::RaftInstallSnapshot);
    EXPECT_EQ(install.last_log_index, 20);
    EXPECT_EQ(install.last_log_term, 9);
    EXPECT_EQ(install.snapshot_payload, "a b 0\n");

    Request status = KVProtocol::parse_request("RAFT_STATUS");
    EXPECT_EQ(status.type, CommandType::RaftStatus);
    EXPECT_TRUE(status.error.empty());

    Request metrics = KVProtocol::parse_request("METRICS");
    EXPECT_EQ(metrics.type, CommandType::Metrics);
    EXPECT_TRUE(metrics.error.empty());
}

TEST(KVProtocolTest, RejectBadResp)
{
    Request request = KVProtocol::parse_request("*2\r\n$3\r\nGET\r\n$6\r\nuser:1");
    EXPECT_EQ(request.protocol, ProtocolType::Resp);
    EXPECT_EQ(request.error, "ERROR invalid_resp");
}
