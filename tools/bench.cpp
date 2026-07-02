#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <numeric>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace
{
struct Options
{
    std::string host = "127.0.0.1";
    int port = 9006;
    int requests = 10000;
    int clients = 50;
};

std::string arg_value(int argc, char **argv, const std::string &name, const std::string &fallback)
{
    for (int i = 1; i + 1 < argc; ++i)
    {
        if (argv[i] == name)
        {
            return argv[i + 1];
        }
    }
    return fallback;
}

Options parse_options(int argc, char **argv)
{
    Options options;
    options.host = arg_value(argc, argv, "--host", options.host);
    options.port = std::stoi(arg_value(argc, argv, "--port", std::to_string(options.port)));
    options.requests = std::stoi(arg_value(argc, argv, "--requests", std::to_string(options.requests)));
    options.clients = std::stoi(arg_value(argc, argv, "--clients", std::to_string(options.clients)));
    options.requests = std::max(1, options.requests);
    options.clients = std::max(1, options.clients);
    return options;
}

int connect_server(const Options &options)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(options.port);
    if (inet_pton(AF_INET, options.host.c_str(), &addr.sin_addr) != 1)
    {
        close(fd);
        return -1;
    }

    if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        close(fd);
        return -1;
    }
    return fd;
}

bool send_all(int fd, const std::string &data)
{
    size_t sent = 0;
    while (sent < data.size())
    {
        ssize_t n = send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n <= 0)
        {
            return false;
        }
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool recv_resp(int fd)
{
    std::string response;
    char ch = '\0';
    while (recv(fd, &ch, 1, 0) == 1)
    {
        response.push_back(ch);
        if (response.size() >= 2 && response.substr(response.size() - 2) == "\r\n")
        {
            break;
        }
    }
    if (response.empty())
    {
        return false;
    }

    if (response[0] == '$')
    {
        long len = std::stol(response.substr(1, response.size() - 3));
        if (len < 0)
        {
            return true;
        }
        size_t need = static_cast<size_t>(len) + 2;
        while (need > 0)
        {
            char buffer[4096];
            ssize_t n = recv(fd, buffer, std::min(sizeof(buffer), need), 0);
            if (n <= 0)
            {
                return false;
            }
            need -= static_cast<size_t>(n);
        }
    }
    return response[0] != '-';
}

std::string set_command(int index)
{
    std::string key = "bench:" + std::to_string(index);
    std::string value = "value:" + std::to_string(index);
    return "*3\r\n$3\r\nSET\r\n$" + std::to_string(key.size()) + "\r\n" + key +
           "\r\n$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}

std::string get_command(int index)
{
    std::string key = "bench:" + std::to_string(index);
    return "*2\r\n$3\r\nGET\r\n$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
}
}

int main(int argc, char **argv)
{
    Options options = parse_options(argc, argv);
    std::atomic<int> next{0};
    std::atomic<int> ok{0};
    std::atomic<int> failed{0};
    std::mutex latency_mutex;
    std::vector<long long> latencies_us;
    latencies_us.reserve(static_cast<size_t>(options.requests));

    auto started = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    for (int c = 0; c < options.clients; ++c)
    {
        workers.emplace_back([&] {
            int fd = connect_server(options);
            if (fd < 0)
            {
                failed.fetch_add(1);
                return;
            }

            while (true)
            {
                int index = next.fetch_add(1);
                if (index >= options.requests)
                {
                    break;
                }

                std::string command = (index % 2 == 0) ? set_command(index) : get_command(index - 1);
                auto begin = std::chrono::steady_clock::now();
                bool success = send_all(fd, command) && recv_resp(fd);
                auto end = std::chrono::steady_clock::now();

                if (success)
                {
                    ok.fetch_add(1);
                }
                else
                {
                    failed.fetch_add(1);
                }

                long long us = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
                std::lock_guard<std::mutex> lock(latency_mutex);
                latencies_us.push_back(us);
            }

            close(fd);
        });
    }

    for (auto &worker : workers)
    {
        worker.join();
    }
    auto finished = std::chrono::steady_clock::now();

    double seconds = std::chrono::duration<double>(finished - started).count();
    std::sort(latencies_us.begin(), latencies_us.end());
    double avg_us = 0.0;
    if (!latencies_us.empty())
    {
        avg_us = static_cast<double>(std::accumulate(latencies_us.begin(), latencies_us.end(), 0LL)) /
                 static_cast<double>(latencies_us.size());
    }
    auto percentile = [&](double p) -> long long {
        if (latencies_us.empty())
        {
            return 0;
        }
        size_t index = static_cast<size_t>(p * static_cast<double>(latencies_us.size() - 1));
        return latencies_us[index];
    };

    std::cout << "requests: " << options.requests << '\n'
              << "clients: " << options.clients << '\n'
              << "success: " << ok.load() << '\n'
              << "failed: " << failed.load() << '\n'
              << "qps: " << static_cast<long long>(static_cast<double>(ok.load()) / seconds) << '\n'
              << "avg_latency_us: " << static_cast<long long>(avg_us) << '\n'
              << "p95_latency_us: " << percentile(0.95) << '\n'
              << "p99_latency_us: " << percentile(0.99) << '\n';

    return failed.load() == 0 ? 0 : 1;
}
