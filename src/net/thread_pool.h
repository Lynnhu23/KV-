#ifndef NET_THREAD_POOL_H
#define NET_THREAD_POOL_H

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool
{
public:
    explicit ThreadPool(size_t thread_count = 0);
    ~ThreadPool();

    ThreadPool(const ThreadPool &) = delete;
    ThreadPool &operator=(const ThreadPool &) = delete;

    bool start();
    void stop();
    bool submit(std::function<void()> task);
    size_t size() const;

private:
    void worker_loop();

private:
    size_t m_thread_count;
    mutable std::mutex m_mutex;
    std::condition_variable m_cond;
    std::queue<std::function<void()>> m_tasks;
    std::vector<std::jthread> m_workers;
    bool m_stopping;
};

#endif
