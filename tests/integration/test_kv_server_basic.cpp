#include <gtest/gtest.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <thread>
#include <chrono>

// 集成测试: 需要 kvserver 在后台运行
class KVServerIntegrationTest : public ::testing::Test
{
protected:
    static constexpr int TEST_PORT = 19006;

    std::string send_command(const std::string &command, int port = TEST_PORT)
    {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return "";

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

        if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        {
            close(sock);
            return "";
        }

        send(sock, command.c_str(), command.size(), 0);

        char buf[4096];
        memset(buf, 0, sizeof(buf));
        recv(sock, buf, sizeof(buf) - 1, 0);
        close(sock);

        return std::string(buf);
    }
};

TEST_F(KVServerIntegrationTest, DISABLED_Ping)
{
    // 此测试需要服务器在后台运行
    // 使用方法: 先启动 ./kvserver -p 19006 再运行测试
    std::string response = send_command("PING\n");

    EXPECT_NE(response.find("PONG"), std::string::npos);
}

TEST_F(KVServerIntegrationTest, DISABLED_PutGet)
{
    std::string response = send_command("PUT user:42 alice\nGET user:42\n");

    EXPECT_NE(response.find("OK"), std::string::npos);
    EXPECT_NE(response.find("VALUE alice"), std::string::npos);
}

TEST_F(KVServerIntegrationTest, DISABLED_Delete)
{
    std::string response = send_command("PUT tmp value\nDEL tmp\nGET tmp\n");

    EXPECT_NE(response.find("OK"), std::string::npos);
    EXPECT_NE(response.find("NOT_FOUND"), std::string::npos);
}
