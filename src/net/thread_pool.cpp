#include "thread_pool.h"

#include <algorithm>

ThreadPool::ThreadPool(size_t thread_count)
    : m_thread_count(thread_count == 0 ? std::max(1u, std::thread::hardware_concurrency()) : thread_count),
      m_stopping(false)
{
}

ThreadPool::~ThreadPool()
{
    stop();
}

bool ThreadPool::start()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_workers.empty())
    {
        return true;
    }

    m_stopping = false;
    try
    {
        for (size_t i = 0; i < m_thread_count; ++i)
        {
            m_workers.emplace_back(&ThreadPool::worker_loop, this);
        }
    }
    catch (...)
    {
        m_stopping = true;
        m_cond.notify_all();
        return false;
    }
    return true;
}

void ThreadPool::stop()
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_stopping = true;
    }
    m_cond.notify_all();
    m_workers.clear();
}

bool ThreadPool::submit(std::function<void()> task)
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_stopping)
        {
            return false;
        }
        m_tasks.push(std::move(task));
    }
    m_cond.notify_one();
    return true;
}

size_t ThreadPool::size() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_thread_count;
}

void ThreadPool::worker_loop()
{
    while (true)
    {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cond.wait(lock, [this] { return m_stopping || !m_tasks.empty(); });
            if (m_stopping && m_tasks.empty())
            {
                return;
            }
            task = std::move(m_tasks.front());
            m_tasks.pop();
        }
        task();
    }
}
