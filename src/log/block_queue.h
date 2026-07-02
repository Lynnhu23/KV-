/*************************************************************
* 循环数组实现的阻塞队列，m_back = (m_back + 1) % m_max_size;
* 线程安全，C++20 重构: 使用 std::mutex + std::condition_variable
**************************************************************/

#ifndef BLOCK_QUEUE_H
#define BLOCK_QUEUE_H

#include <iostream>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <chrono>

template <class T>
class block_queue
{
public:
    explicit block_queue(int max_size = 1000)
    {
        if (max_size <= 0)
        {
            exit(-1);
        }

        m_max_size = max_size;
        m_array = std::make_unique<T[]>(max_size);
        m_size = 0;
        m_front = -1;
        m_back = -1;
        m_closed = false;
    }

    void clear()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_size = 0;
        m_front = -1;
        m_back = -1;
    }

    ~block_queue() = default;  // unique_ptr auto-destructs

    void close()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_closed = true;
        m_cond.notify_all();
    }

    // 判断队列是否满了
    bool full()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_size >= m_max_size;
    }

    // 判断队列是否为空
    bool empty()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return 0 == m_size;
    }

    // 返回队首元素 (m_front 指向下次 pop 的位置，当前队首在 +1 处)
    bool front(T &value)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (0 == m_size)
            return false;
        value = m_array[(m_front + 1) % m_max_size];
        return true;
    }

    // 返回队尾元素
    bool back(T &value)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (0 == m_size)
            return false;
        value = m_array[m_back];
        return true;
    }

    int size()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_size;
    }

    int max_size()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_max_size;
    }

    // 往队列添加元素
    // 当有元素push进队列,相当于生产者生产了一个元素
    bool push(const T &item)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_closed)
            return false;
        if (m_size >= m_max_size)
            return false;

        m_back = (m_back + 1) % m_max_size;
        m_array[m_back] = item;
        ++m_size;

        m_cond.notify_one();
        return true;
    }

    // pop时,如果当前队列没有元素,将会等待条件变量
    bool pop(T &item)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cond.wait(lock, [this] { return m_size > 0 || m_closed; });

        if (m_size == 0 && m_closed)
            return false;

        m_front = (m_front + 1) % m_max_size;
        item = m_array[m_front];
        --m_size;
        return true;
    }

    // 增加了超时处理（毫秒）
    bool pop(T &item, int ms_timeout)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (!m_cond.wait_for(lock, std::chrono::milliseconds(ms_timeout),
                             [this] { return m_size > 0 || m_closed; }))
        {
            return false;
        }

        if (m_size == 0 && m_closed)
            return false;

        m_front = (m_front + 1) % m_max_size;
        item = m_array[m_front];
        --m_size;
        return true;
    }

private:
    std::mutex m_mutex;
    std::condition_variable m_cond;

    std::unique_ptr<T[]> m_array;
    int m_size;
    int m_max_size;
    int m_front;
    int m_back;
    bool m_closed;
};

#endif
