#include <cstring>
#include <cstdarg>
#include <cstdio>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <memory>
#include <thread>
#include <vector>
#include "log.h"

using namespace std;
namespace fs = std::filesystem;

Log::Log()
{
    m_split_lines = 0;
    m_log_buf_size = 0;
    m_count = 0;
    m_today = 0;
    m_fp = NULL;
    m_initialized = false;
    m_is_async = false;
    m_close_log = 0;
}

Log::~Log()
{
    if (m_log_queue)
    {
        m_log_queue->close();
    }
    if (m_async_thread.joinable())
    {
        m_async_thread.join();
    }

    if (m_fp != NULL)
    {
        fclose(m_fp);
    }
}

// 异步需要设置阻塞队列的长度，同步不需要设置
bool Log::init(const char *file_name, int close_log, int log_buf_size, int split_lines, int max_queue_size)
{
    // 如果设置了max_queue_size,则设置为异步
    if (max_queue_size >= 1)
    {
        m_is_async = true;
        m_log_queue = make_unique<block_queue<string>>(max_queue_size);
        // C++20: std::jthread 替代 pthread_create
        m_async_thread = std::jthread(&Log::async_write_log, this);
    }

    m_close_log = close_log;
    m_log_buf_size = log_buf_size;
    m_buf.resize(m_log_buf_size);
    fill(m_buf.begin(), m_buf.end(), '\0');
    m_split_lines = split_lines;

    auto now = chrono::system_clock::now();
    auto time_t_now = chrono::system_clock::to_time_t(now);
    struct tm my_tm;
    localtime_r(&time_t_now, &my_tm);

    fs::path file_path(file_name);
    dir_name = file_path.parent_path().string();
    if (!dir_name.empty()) dir_name += "/";
    log_name = file_path.filename().string();

    m_today = my_tm.tm_mday;

    char buf[512];
    snprintf(buf, sizeof(buf), "%s%04d_%02d_%02d_%s",
             dir_name.c_str(), my_tm.tm_year + 1900,
             my_tm.tm_mon + 1, my_tm.tm_mday, log_name.c_str());
    string log_full_name(buf);

    m_fp = fopen(log_full_name.c_str(), "a");
    if (m_fp == NULL)
    {
        m_initialized = false;
        return false;
    }

    m_initialized = true;
    return true;
}

void Log::write_log(int level, const char *format, ...)
{
    // 若关闭日志则直接返回
    if (m_close_log) return;

    if (!m_initialized || m_fp == NULL || m_buf.empty())
    {
        va_list valst;
        va_start(valst, format);
        std::lock_guard<std::mutex> lock(m_mutex);
        vfprintf(stderr, format, valst);
        fputc('\n', stderr);
        va_end(valst);
        return;
    }

    auto now = chrono::system_clock::now();
    auto time_t_now = chrono::system_clock::to_time_t(now);
    auto us = chrono::duration_cast<chrono::microseconds>(
        now.time_since_epoch()) % 1'000'000;
    struct tm my_tm;
    localtime_r(&time_t_now, &my_tm);

    const char *level_str = "[info]:";
    switch (level)
    {
    case 0: level_str = "[debug]:"; break;
    case 1: level_str = "[info]:";  break;
    case 2: level_str = "[warn]:";  break;
    case 3: level_str = "[erro]:";  break;
    }

    // 写入一个log，对m_count++
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_count++;

        if (m_today != my_tm.tm_mday || m_count % m_split_lines == 0)
        {
            fflush(m_fp);
            fclose(m_fp);
            char name_buf[512];
            snprintf(name_buf, sizeof(name_buf), "%s%04d_%02d_%02d_%s",
                     dir_name.c_str(), my_tm.tm_year + 1900,
                     my_tm.tm_mon + 1, my_tm.tm_mday, log_name.c_str());
            string new_log(name_buf);
            if (m_today != my_tm.tm_mday)
            {
                m_today = my_tm.tm_mday;
                m_count = 0;
            }
            else
            {
                snprintf(name_buf, sizeof(name_buf), ".%zu", m_count / m_split_lines);
                new_log += name_buf;
            }
            m_fp = fopen(new_log.c_str(), "a");
        }
    }

    va_list valst;
    va_start(valst, format);

    string log_str;
    {
        std::lock_guard<std::mutex> lock(m_mutex);

        // 构建时间头 (snprintf 替代 std::format 以兼容较老编译器)
        char header[128];
        int n = snprintf(header, sizeof(header),
                         "%04d-%02d-%02d %02d:%02d:%02d.%06d %s ",
                         my_tm.tm_year + 1900, my_tm.tm_mon + 1, my_tm.tm_mday,
                         my_tm.tm_hour, my_tm.tm_min, my_tm.tm_sec,
                         static_cast<int>(us.count()), level_str);

        // TODO: C++23 std::print 可直接格式化 va_list
        // 当前保留 vsnprintf 作为桥梁
        memcpy(m_buf.data(), header, n);
        int m = vsnprintf(m_buf.data() + n, m_log_buf_size - n - 1, format, valst);
        m_buf[n + m] = '\n';
        m_buf[n + m + 1] = '\0';
        log_str = m_buf.data();
    }

    if (m_is_async && m_log_queue && !m_log_queue->full())
    {
        m_log_queue->push(log_str);
    }
    else
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        fputs(log_str.c_str(), m_fp);
    }

    va_end(valst);
}

void Log::flush(void)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    // 强制刷新写入流缓冲区
    if (m_fp != NULL)
        fflush(m_fp);
}
